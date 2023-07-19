package mysql

import (
	"context"
	"fmt"
	"time"

	"github.com/igxm/fastflow/pkg/mod"
	"github.com/igxm/fastflow/pkg/utils/data"
	"gorm.io/gorm"
)

type LockDetail struct {
	Key       string    `gorm:"column:id" json:"id"`
	ExpiredAt time.Time `json:"expired_at"`
	Identity  string    `json:"identity"`
}

type MongoMutex struct {
	key string

	clsName    string
	db         *gorm.DB
	lockDetail *LockDetail
}

func (m *MongoMutex) Lock(ctx context.Context, ops ...mod.LockOptionOp) error {
	opt := mod.NewLockOption(ops)
	if err := m.spinLock(ctx, opt); err != nil {
		return err
	}
	// already keep lock
	if m.lockDetail != nil {
		return nil
	}

	// when get lock failed, loop to get it
	ticker := time.NewTicker(opt.SpinInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if err := m.spinLock(ctx, opt); err != nil {
				return nil
			}
			// already keep lock
			if m.lockDetail != nil {
				return nil
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
}

func (m *MongoMutex) spinLock(ctx context.Context, opt *mod.LockOption) error {
	detail := LockDetail{}
	err := m.db.Table(m.clsName).Where("id = ?", m.key).First(&detail).Error
	if err != nil && err != gorm.ErrRecordNotFound {
		return fmt.Errorf("get lock detail failed: %w", err)
	}

	// no lock
	if err == gorm.ErrRecordNotFound {
		d := &LockDetail{
			Key:       m.key,
			ExpiredAt: time.Now().Add(opt.TTL),
			Identity:  opt.ReentrantIdentity,
		}
		err := m.db.Table(m.clsName).Create(d).Error
		if err != nil {
			if err == gorm.ErrPrimaryKeyRequired {
				// race lock failed, ready to get lock next time
				return nil
			}
			return fmt.Errorf("insert lock detail failed: %w", err)
		}
		m.lockDetail = d
		return nil
	}

	// lock existed, we should check it is expired
	if detail.ExpiredAt.Before(time.Now()) {
		exp := time.Now().Add(opt.TTL)
		ret := m.db.Table(m.clsName).Where("id = ? and expired_at = ?", m.key, detail.ExpiredAt).Updates(map[string]any{
			"expired_at": time.Now().Add(opt.TTL),
			"identity":   opt.ReentrantIdentity,
		})
		if ret.Error != nil {
			return fmt.Errorf("get lock failed: %w", ret.Error)
		}
		// lock is keep by others
		if ret.RowsAffected == 0 {
			return nil
		}
		detail.ExpiredAt = exp
		m.lockDetail = &detail
		return nil
	}

	// lock existed, we should check it is reentrant
	if opt.ReentrantIdentity != "" && detail.Identity == opt.ReentrantIdentity {
		m.lockDetail = &detail
		return nil
	}

	// lock is keep by others, return to loop
	return nil
}

func (m *MongoMutex) Unlock(ctx context.Context) error {
	if m.lockDetail == nil {
		return fmt.Errorf("the mutex is not locked")
	}

	ret := m.db.Exec(fmt.Sprintf("delete from %s where id = ? and expired_at =?", m.clsName), m.key, m.lockDetail.ExpiredAt)
	if ret.Error != nil {
		return fmt.Errorf("delete lock detail failed: %w", ret.Error)
	}

	if ret.RowsAffected == 0 {
		return data.ErrMutexAlreadyUnlock
	}
	m.lockDetail = nil
	return nil
}
