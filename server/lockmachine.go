package server

import "io"

type LockMatchine struct {
}

func newLockMachine() *LockMatchine {
	lm := &LockMatchine{}
	return lm
}

func (lm *LockMatchine) ApplyLog(log []byte) {

}
func (lm *LockMatchine) Snapshot(w io.Writer) error {
	return nil
}

func (lm *LockMatchine) InstallSnapshot(r io.Reader) error {
	return nil
}
