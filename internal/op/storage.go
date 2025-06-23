package op

import (
	"context"
	"fmt"
	"runtime"
	"sort"
	"strings"
	"time"

	"github.com/OpenListTeam/OpenList/internal/db"
	"github.com/OpenListTeam/OpenList/internal/driver"
	"github.com/OpenListTeam/OpenList/internal/errs"
	"github.com/OpenListTeam/OpenList/internal/model"
	"github.com/OpenListTeam/OpenList/pkg/generic_sync"
	"github.com/OpenListTeam/OpenList/pkg/utils"
	mapset "github.com/deckarep/golang-set/v2"
	"github.com/pkg/errors"
	log "github.com/sirupsen/logrus"
)

// Although the driver type is stored,
// there is a storage in each driver,
// so it should actually be a storage, just wrapped by the driver
var storagesMap generic_sync.MapOf[string, driver.Driver]
var reconnectCancelFuncs generic_sync.MapOf[string, context.CancelFunc] // 用于存储重连goroutine的取消函数

// ReconnectTask 存储重连任务的状态
type ReconnectTask struct {
	Storage     model.Storage
	Driver      driver.Driver
	RetryCount  int
	NextRetryAt time.Time
	Cancel      context.CancelFunc // 用于取消单个任务的重连
}

// reconnectScheduler 全局重连调度器
type reconnectScheduler struct {
	tasks generic_sync.MapOf[string, *ReconnectTask] // mountPath -> ReconnectTask
	queue chan string                               // 待处理的mountPath
	ctx   context.Context
	cancel context.CancelFunc
}

var globalReconnectScheduler *reconnectScheduler

func init() {
	globalReconnectScheduler = newReconnectScheduler()
	go globalReconnectScheduler.Start()
}

func newReconnectScheduler() *reconnectScheduler {
	ctx, cancel := context.WithCancel(context.Background())
	return &reconnectScheduler{
		tasks:  generic_sync.MapOf[string, *ReconnectTask]{},
		queue:  make(chan string, 100), // 缓冲通道，避免阻塞
		ctx:    ctx,
		cancel: cancel,
	}
}

func (s *reconnectScheduler) Start() {
	log.Info("Reconnect scheduler started.")
	ticker := time.NewTicker(1 * time.Second) // 每秒检查一次
	defer ticker.Stop()

	for {
		select {
		case <-s.ctx.Done():
			log.Info("Reconnect scheduler stopped.")
			return
		case mountPath := <-s.queue:
			s.processReconnectTask(mountPath)
		case <-ticker.C:
			s.checkAndScheduleRetries()
		}
	}
}

func (s *reconnectScheduler) AddOrUpdateTask(storage model.Storage, storageDriver driver.Driver) {
	task, loaded := s.tasks.Load(storage.MountPath)
	if loaded {
		// 如果任务已存在，更新其信息并重置重试计数
		task.Storage = storage
		task.Driver = storageDriver
		task.RetryCount = 0
		task.NextRetryAt = time.Now() // 立即重试
		task.Cancel() // 取消旧的重试，重新开始
	} else {
		// 新任务
		task = &ReconnectTask{
			Storage:    storage,
			Driver:     storageDriver,
			RetryCount: 0,
		}
	}
	s.tasks.Store(storage.MountPath, task)
	s.queue <- storage.MountPath // 将任务加入队列
}

func (s *reconnectScheduler) RemoveTask(mountPath string) {
	if task, loaded := s.tasks.LoadAndDelete(mountPath); loaded {
		if task.Cancel != nil {
			task.Cancel() // 取消正在进行的重试
		}
		log.Infof("Removed reconnect task for storage: %s", mountPath)
	}
}

func (s *reconnectScheduler) processReconnectTask(mountPath string) {
	task, loaded := s.tasks.Load(mountPath)
	if !loaded {
		return
	}

	reconnectErr := initStorage(context.Background(), task.Storage, task.Driver)
	if reconnectErr == nil {
		log.Infof("Successfully reconnected storage: %s", task.Storage.MountPath)
		s.RemoveTask(mountPath) // 成功后移除任务
		return
	}
	log.Errorf("Failed to reconnect storage %s: %+v", task.Storage.MountPath, reconnectErr)

	task.RetryCount++
	if task.RetryCount >= task.Storage.AutoReconnectMaxAttempts {
		log.Warnf("Max auto-reconnect attempts reached for storage %s. Stopping retries.", task.Storage.MountPath)
		s.RemoveTask(mountPath) // 达到最大重试次数，停止重连
		return
	}

	// 计算下一次重试间隔（指数退避 + 抖动）
	initialInterval := time.Duration(task.Storage.AutoReconnectInitialInterval) * time.Second
	if initialInterval <= 0 {
		initialInterval = 30 * time.Second // 默认初始间隔30秒
	}
	currentInterval := initialInterval * time.Duration(1<<task.RetryCount) // 指数增长
	jitter := time.Duration(utils.RandInt(0, int(currentInterval.Seconds()/10))) * time.Second
	sleepDuration := currentInterval + jitter

	task.NextRetryAt = time.Now().Add(sleepDuration)
	log.Infof("Scheduling next reconnect for storage %s in %s (attempt %d/%d)", task.Storage.MountPath, sleepDuration, task.RetryCount+1, task.Storage.AutoReconnectMaxAttempts)
}

func (s *reconnectScheduler) checkAndScheduleRetries() {
	now := time.Now()
	s.tasks.Range(func(mountPath string, task *ReconnectTask) bool {
		if now.After(task.NextRetryAt) {
			select {
			case s.queue <- mountPath: // 将到期的任务重新加入队列
				// Successfully added to the queue
			default:
				log.Warnf("Queue is full, unable to schedule reconnect task for storage: %s", mountPath)
			}
		}
		return true
	})
}

func GetAllStorages() []driver.Driver {
	return storagesMap.Values()
}

func HasStorage(mountPath string) bool {
	return storagesMap.Has(utils.FixAndCleanPath(mountPath))
}

func GetStorageByMountPath(mountPath string) (driver.Driver, error) {
	mountPath = utils.FixAndCleanPath(mountPath)
	storageDriver, ok := storagesMap.Load(mountPath)
	if !ok {
		return nil, errors.Errorf("no mount path for an storage is: %s", mountPath)
	}
	return storageDriver, nil
}

// CreateStorage Save the storage to database so storage can get an id
// then instantiate corresponding driver and save it in memory
func CreateStorage(ctx context.Context, storage model.Storage) (uint, error) {
	storage.Modified = time.Now()
	storage.MountPath = utils.FixAndCleanPath(storage.MountPath)
	var err error
	// check driver first
	driverName := storage.Driver
	driverNew, err := GetDriver(driverName)
	if err != nil {
		return 0, errors.WithMessage(err, "failed get driver new")
	}
	storageDriver := driverNew()
	// insert storage to database
	err = db.CreateStorage(&storage)
	if err != nil {
		return storage.ID, errors.WithMessage(err, "failed create storage in database")
	}
	// already has an id
	err = initStorage(ctx, storage, storageDriver)
	go callStorageHooks("add", storageDriver)
	if err != nil {
		return storage.ID, errors.Wrap(err, "failed init storage but storage is already created")
	}
	log.Debugf("storage %+v is created", storageDriver)
	return storage.ID, nil
}

// LoadStorage load exist storage in db to memory
func LoadStorage(ctx context.Context, storage model.Storage) error {
	storage.MountPath = utils.FixAndCleanPath(storage.MountPath)
	// check driver first
	driverName := storage.Driver
	driverNew, err := GetDriver(driverName)
	if err != nil {
		return errors.WithMessage(err, "failed get driver new")
	}
	storageDriver := driverNew()

	err = initStorage(ctx, storage, storageDriver)
	go callStorageHooks("add", storageDriver)
	log.Debugf("storage %+v is created", storageDriver)

	// Add auto-reconnect logic
	if err != nil && storage.AutoReconnectEnabled { // 检查是否启用自动重连
		globalReconnectScheduler.AddOrUpdateTask(storage, storageDriver)
	} else {
		// 如果AutoReconnectEnabled 为 false，确保没有重连任务在调度器中
		globalReconnectScheduler.RemoveTask(storage.MountPath)
	}
	return err
}

func getCurrentGoroutineStack() string {
	buf := make([]byte, 1<<16)
	n := runtime.Stack(buf, false)
	return string(buf[:n])
}

// initStorage initialize the driver and store to storagesMap
func initStorage(ctx context.Context, storage model.Storage, storageDriver driver.Driver) (err error) {
	storageDriver.SetStorage(storage)
	driverStorage := storageDriver.GetStorage()
	defer func() {
		if err := recover(); err != nil {
			errInfo := fmt.Sprintf("[panic] err: %v\nstack: %s\n", err, getCurrentGoroutineStack())
			log.Errorf("panic init storage: %s", errInfo)
			driverStorage.SetStatus(errInfo)
			MustSaveDriverStorage(storageDriver)
			storagesMap.Store(driverStorage.MountPath, storageDriver)
		}
	}()
	// Unmarshal Addition
	err = utils.Json.UnmarshalFromString(driverStorage.Addition, storageDriver.GetAddition())
	if err == nil {
		if ref, ok := storageDriver.(driver.Reference); ok {
			if strings.HasPrefix(driverStorage.Remark, "ref:/") {
				refMountPath := driverStorage.Remark
				i := strings.Index(refMountPath, "\n")
				if i > 0 {
					refMountPath = refMountPath[4:i]
				} else {
					refMountPath = refMountPath[4:]
				}
				var refStorage driver.Driver
				refStorage, err = GetStorageByMountPath(refMountPath)
				if err != nil {
					err = fmt.Errorf("ref: %w", err)
				} else {
					err = ref.InitReference(refStorage)
					if err != nil && errs.IsNotSupportError(err) {
						err = fmt.Errorf("ref: storage is not %s", storageDriver.Config().Name)
					}
				}
			}
		}
	}
	if err == nil {
		err = storageDriver.Init(ctx)
	}
	storagesMap.Store(driverStorage.MountPath, storageDriver)
	if err != nil {
		driverStorage.SetStatus(err.Error())
		err = errors.Wrap(err, "failed init storage")
	} else {
		driverStorage.SetStatus(WORK)
	}
	MustSaveDriverStorage(storageDriver)
	return err
}

func EnableStorage(ctx context.Context, id uint) error {
	storage, err := db.GetStorageById(id)
	if err != nil {
		return errors.WithMessage(err, "failed get storage")
	}
	if !storage.Disabled {
		return errors.Errorf("this storage have enabled")
	}
	storage.Disabled = false
	err = db.UpdateStorage(storage)
	if err != nil {
		return errors.WithMessage(err, "failed update storage in db")
	}
	err = LoadStorage(ctx, *storage)
	if err != nil {
		return errors.WithMessage(err, "failed load storage")
	}
	return nil
}

func DisableStorage(ctx context.Context, id uint) error {
	storage, err := db.GetStorageById(id)
	if err != nil {
		return errors.WithMessage(err, "failed get storage")
	}
	if storage.Disabled {
		return errors.Errorf("this storage have disabled")
	}
	storageDriver, err := GetStorageByMountPath(storage.MountPath)
	if err != nil {
		return errors.WithMessage(err, "failed get storage driver")
	}
	// drop the storage in the driver
	if err := storageDriver.Drop(ctx); err != nil {
		return errors.Wrap(err, "failed drop storage")
	}
	// delete the storage in the memory
	storage.Disabled = true
	storage.SetStatus(DISABLED)
	err = db.UpdateStorage(storage)
	if err != nil {
		return errors.WithMessage(err, "failed update storage in db")
	}
	storagesMap.Delete(storage.MountPath)
	globalReconnectScheduler.RemoveTask(storage.MountPath) // 移除重连任务
	go callStorageHooks("del", storageDriver)
	return nil
}

// UpdateStorage update storage
// get old storage first
// drop the storage then reinitialize
func UpdateStorage(ctx context.Context, storage model.Storage) error {
	oldStorage, err := db.GetStorageById(storage.ID)
	if err != nil {
		return errors.WithMessage(err, "failed get old storage")
	}
	if oldStorage.Driver != storage.Driver {
		return errors.Errorf("driver cannot be changed")
	}
	storage.Modified = time.Now()
	storage.MountPath = utils.FixAndCleanPath(storage.MountPath)
	err = db.UpdateStorage(&storage)
	if err != nil {
		return errors.WithMessage(err, "failed update storage in database")
	}
	if storage.Disabled {
		return nil
	}
	storageDriver, err := GetStorageByMountPath(oldStorage.MountPath)
	if oldStorage.MountPath != storage.MountPath {
		// mount path renamed, need to drop the storage
		storagesMap.Delete(oldStorage.MountPath)
	}
	if err != nil {
		return errors.WithMessage(err, "failed get storage driver")
	}
	err = storageDriver.Drop(ctx)
	if err != nil {
		return errors.Wrapf(err, "failed drop storage")
	}

	err = initStorage(ctx, storage, storageDriver)
	go callStorageHooks("update", storageDriver)
	log.Debugf("storage %+v is update", storageDriver)
	return err
}

func DeleteStorageById(ctx context.Context, id uint) error {
	storage, err := db.GetStorageById(id)
	if err != nil {
		return errors.WithMessage(err, "failed get storage")
	}
	if !storage.Disabled {
		storageDriver, err := GetStorageByMountPath(storage.MountPath)
		if err != nil {
			return errors.WithMessage(err, "failed get storage driver")
		}
		// drop the storage in the driver
		if err := storageDriver.Drop(ctx); err != nil {
			return errors.Wrapf(err, "failed drop storage")
		}
		// delete the storage in the memory
		storagesMap.Delete(storage.MountPath)
		globalReconnectScheduler.RemoveTask(storage.MountPath) // 移除重连任务
		go callStorageHooks("del", storageDriver)
	}
	// delete the storage in the database
	if err := db.DeleteStorageById(id); err != nil {
		return errors.WithMessage(err, "failed delete storage in database")
	}
	return nil
}

// MustSaveDriverStorage call from specific driver
func MustSaveDriverStorage(driver driver.Driver) {
	err := saveDriverStorage(driver)
	if err != nil {
		log.Errorf("failed save driver storage: %s", err)
	}
}

func saveDriverStorage(driver driver.Driver) error {
	storage := driver.GetStorage()
	addition := driver.GetAddition()
	str, err := utils.Json.MarshalToString(addition)
	if err != nil {
		return errors.Wrap(err, "error while marshal addition")
	}
	storage.Addition = str
	err = db.UpdateStorage(storage)
	if err != nil {
		return errors.WithMessage(err, "failed update storage in database")
	}
	return nil
}

// getStoragesByPath get storage by longest match path, contains balance storage.
// for example, there is /a/b,/a/c,/a/d/e,/a/d/e.balance
// getStoragesByPath(/a/d/e/f) => /a/d/e,/a/d/e.balance
func getStoragesByPath(path string) []driver.Driver {
	storages := make([]driver.Driver, 0)
	curSlashCount := 0
	storagesMap.Range(func(mountPath string, value driver.Driver) bool {
		mountPath = utils.GetActualMountPath(mountPath)
		// is this path
		if utils.IsSubPath(mountPath, path) {
			slashCount := strings.Count(utils.PathAddSeparatorSuffix(mountPath), "/")
			// not the longest match
			if slashCount > curSlashCount {
				storages = storages[:0]
				curSlashCount = slashCount
			}
			if slashCount == curSlashCount {
				storages = append(storages, value)
			}
		}
		return true
	})
	// make sure the order is the same for same input
	sort.Slice(storages, func(i, j int) bool {
		return storages[i].GetStorage().MountPath < storages[j].GetStorage().MountPath
	})
	return storages
}

// GetStorageVirtualFilesByPath Obtain the virtual file generated by the storage according to the path
// for example, there are: /a/b,/a/c,/a/d/e,/a/b.balance1,/av
// GetStorageVirtualFilesByPath(/a) => b,c,d
func GetStorageVirtualFilesByPath(prefix string) []model.Obj {
	files := make([]model.Obj, 0)
	storages := storagesMap.Values()
	sort.Slice(storages, func(i, j int) bool {
		if storages[i].GetStorage().Order == storages[j].GetStorage().Order {
			return storages[i].GetStorage().MountPath < storages[j].GetStorage().MountPath
		}
		return storages[i].GetStorage().Order < storages[j].GetStorage().Order
	})

	prefix = utils.FixAndCleanPath(prefix)
	set := mapset.NewSet[string]()
	for _, v := range storages {
		mountPath := utils.GetActualMountPath(v.GetStorage().MountPath)
		// Exclude prefix itself and non prefix
		if len(prefix) >= len(mountPath) || !utils.IsSubPath(prefix, mountPath) {
			continue
		}
		name := strings.SplitN(strings.TrimPrefix(mountPath[len(prefix):], "/"), "/", 2)[0]
		if set.Add(name) {
			files = append(files, &model.Object{
				Name:     name,
				Size:     0,
				Modified: v.GetStorage().Modified,
				IsFolder: true,
			})
		}
	}
	return files
}

var balanceMap generic_sync.MapOf[string, int]

// GetBalancedStorage get storage by path
func GetBalancedStorage(path string) driver.Driver {
	path = utils.FixAndCleanPath(path)
	storages := getStoragesByPath(path)
	storageNum := len(storages)
	switch storageNum {
	case 0:
		return nil
	case 1:
		return storages[0]
	default:
		virtualPath := utils.GetActualMountPath(storages[0].GetStorage().MountPath)
		i, _ := balanceMap.LoadOrStore(virtualPath, 0)
		i = (i + 1) % storageNum
		balanceMap.Store(virtualPath, i)
		return storages[i]
	}
}
