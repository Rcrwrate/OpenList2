package task

type Lifecycle interface {
	BeforeRun() error
	RunCore() error
	AfterRun(err error) error
}

func RunWithLifecycle(t Lifecycle) error {
	if err := t.BeforeRun(); err != nil {
		return err
	}
	err := t.RunCore()
	return t.AfterRun(err)
}
