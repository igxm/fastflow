package mysql

import (
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/igxm/fastflow/keeper"
	"github.com/igxm/fastflow/pkg/event"
	"github.com/igxm/fastflow/pkg/log"
	"github.com/igxm/fastflow/pkg/mod"
	"github.com/igxm/fastflow/store"
	"github.com/shiningrush/goevent"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

const LeaderKey = "leader"

// Keeper mongo implement
type Keeper struct {
	opt              *KeeperOption
	leaderClsName    string
	heartbeatClsName string
	mutexClsName     string

	leaderFlag atomic.Value
	keyNumber  int

	db *gorm.DB

	wg            sync.WaitGroup
	firstInitWg   sync.WaitGroup
	initCompleted bool
	closeCh       chan struct{}
}

// KeeperOption
type KeeperOption struct {
	DB *gorm.DB
	// Key the work key, must be the format like "xxxx-{{number}}", number is the code of worker
	Key string
	// mysql connection string
	Dsn string
	// the prefix will append to the database
	Prefix string
	// UnhealthyTime default 5s, campaign and heartbeat time will be half of it
	UnhealthyTime time.Duration
	// Timeout default 2s
	Timeout time.Duration
}

// NewKeeper
func NewKeeper(opt *KeeperOption) *Keeper {
	k := &Keeper{
		opt:     opt,
		closeCh: make(chan struct{}),
	}
	k.leaderFlag.Store(false)
	return k
}

// Init
func (k *Keeper) Init() error {
	if err := k.readOpt(); err != nil {
		return err
	}
	store.InitFlakeGenerator(uint16(k.WorkerNumber()))

	if k.opt.DB == nil {
		mysqlConfig := mysql.Config{
			DSN:                       k.opt.Dsn, // DSN data source name
			DisableDatetimePrecision:  true,      // 禁用 datetime 精度，MySQL 5.6 之前的数据库不支持
			SkipInitializeWithVersion: false,     // 根据版本自动配置
		}

		db, err := gorm.Open(mysql.New(mysqlConfig), &gorm.Config{
			DisableForeignKeyConstraintWhenMigrating: true,
		})
		if err != nil {
			return err
		}
		k.db = db

	} else {
		k.db = k.opt.DB
	}

	// if err := k.ensureTtlIndex(ctx, k.leaderClsName, "updatedAt", int32(k.opt.UnhealthyTime.Seconds())); err != nil {
	// 	return err
	// }
	// if err := k.ensureTtlIndex(ctx, k.heartbeatClsName, "updatedAt", int32(k.opt.UnhealthyTime.Seconds())); err != nil {
	// 	return err
	// }
	// if err := k.ensureTtlIndex(ctx, k.mutexClsName, "expiredAt", 1); err != nil {
	// 	return err
	// }

	k.firstInitWg.Add(2)

	k.wg.Add(1)
	go k.goElect()
	k.wg.Add(1)
	go k.goHeartBeat()

	k.firstInitWg.Wait()
	k.initCompleted = true
	return nil
}

func (k *Keeper) setLeaderFlag(isLeader bool) {
	k.leaderFlag.Store(isLeader)
	goevent.Publish(&event.LeaderChanged{
		IsLeader:  isLeader,
		WorkerKey: k.WorkerKey(),
	})
}

// func (k *Keeper) ensureTtlIndex(ctx context.Context, clsName, field string, ttl int32) error {
// 	if _, err := k.mongoDb.Collection(clsName).Indexes().CreateOne(ctx, mongo.IndexModel{
// 		Keys: bson.M{
// 			field: 1,
// 		},
// 		Options: &options.IndexOptions{
// 			ExpireAfterSeconds: &ttl,
// 		},
// 	}); err != nil {
// 		isDiffOptErr := false
// 		if driverErr, ok := err.(driver.Error); ok {
// 			// index already existed and option is different
// 			if driverErr.Code == 85 {
// 				isDiffOptErr = true
// 			}
// 		}
// 		if !isDiffOptErr {
// 			return fmt.Errorf("create index failed: %w", err)
// 		}

// 		_, err := k.mongoDb.Collection(clsName).Indexes().DropAll(ctx)
// 		if err != nil {
// 			return fmt.Errorf("drop all index failed: %w", err)
// 		}
// 		return k.ensureTtlIndex(ctx, clsName, field, ttl)
// 	}
// 	return nil
// }

func (k *Keeper) readOpt() error {

	if k.opt.DB == nil {
		if k.opt.Dsn == "" {
			return fmt.Errorf("Dsn string cannot be empty")
		}
	}
	if k.opt.UnhealthyTime == 0 {
		k.opt.UnhealthyTime = time.Second * 5
	}
	if k.opt.Timeout == 0 {
		k.opt.Timeout = time.Second * 2
	}

	number, err := keeper.CheckWorkerKey(k.opt.Key)
	if err != nil {
		return err
	}
	k.keyNumber = number

	k.leaderClsName = "election"
	k.heartbeatClsName = "heartbeat"
	k.mutexClsName = "mutex"
	if k.opt.Prefix != "" {
		k.leaderClsName = fmt.Sprintf("%s_%s", k.opt.Prefix, k.leaderClsName)
		k.heartbeatClsName = fmt.Sprintf("%s_%s", k.opt.Prefix, k.heartbeatClsName)
		k.mutexClsName = fmt.Sprintf("%s_%s", k.opt.Prefix, k.mutexClsName)
	}

	return nil
}

// IsLeader indicate the component if is leader node
func (k *Keeper) IsLeader() bool {
	return k.leaderFlag.Load().(bool)
}

// AliveNodes get all alive nodes
func (k *Keeper) AliveNodes() ([]string, error) {
	var ret []Payload
	if err := k.db.Table(k.heartbeatClsName).Where("updated_at > ?", time.Now().Add(-1*k.opt.UnhealthyTime)).Find(&ret).Error; err != nil {
		return nil, fmt.Errorf("decode failed: %w", err)
	}

	var aliveNodes []string
	for i := range ret {
		aliveNodes = append(aliveNodes, ret[i].WorkerKey)
	}
	return aliveNodes, nil
}

// IsAlive check if a worker still alive
func (k *Keeper) IsAlive(workerKey string) (bool, error) {
	var p Payload
	// mongodb background worker delete expired date every 60s, so can not believe it
	err := k.db.Table(k.heartbeatClsName).Where("id = ? and updated_at > ?", workerKey, time.Now().Add(-1*k.opt.UnhealthyTime)).First(&p).Error
	if err == gorm.ErrRecordNotFound {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("query failed: %w", err)
	}
	return true, nil
}

// WorkerNumber get the the key number of Worker key, if here is a WorkKey like `worker-1`, then it will return "1"
func (k *Keeper) WorkerNumber() int {
	return k.keyNumber
}

// WorkerKey must match `xxxx-1` format
func (k *Keeper) WorkerKey() string {
	return k.opt.Key
}

// NewMutex(key string) create a new distributed mutex
func (k *Keeper) NewMutex(key string) mod.DistributedMutex {
	return &MongoMutex{
		key:     key,
		clsName: k.mutexClsName,
		db:      k.db,
	}
}

// close component
func (k *Keeper) Close() {
	close(k.closeCh)
	k.wg.Wait()

	if k.leaderFlag.Load().(bool) {
		err := k.db.Exec(fmt.Sprintf("delete from %s where id =?", k.leaderClsName), LeaderKey).Error
		if err != nil {
			log.Errorf("deregister leader failed: %s", err)
		}
	}

	err := k.db.Exec(fmt.Sprintf("delete from %s where id =?", k.heartbeatClsName), k.opt.Key).Error
	if err != nil {
		log.Errorf("deregister heart beat failed: %s", err)
	}

}

// this function is just for testing
func (k *Keeper) forceClose() {
	close(k.closeCh)
	k.wg.Wait()
}

// Payload header beat dto
type Payload struct {
	WorkerKey string    `bson:"_id" gorm:"column:id"`
	UpdatedAt time.Time `bson:"updatedAt"`
}

// LeaderPayload leader election dto
type LeaderPayload struct {
	ID        string    `bson:"_id" gorm:"column:id"`
	WorkerKey string    `bson:"workerKey"`
	UpdatedAt time.Time `bson:"updatedAt"`
}

func (k *Keeper) goElect() {
	timerCh := time.Tick(k.opt.UnhealthyTime / 2)
	closed := false
	for !closed {
		select {
		case <-k.closeCh:
			closed = true
		case <-timerCh:
			k.elect()
		}
	}
	k.wg.Done()
}

func (k *Keeper) elect() {
	if k.leaderFlag.Load().(bool) {
		if err := k.continueLeader(); err != nil {
			log.Errorf("continue leader failed: %s", err)
			k.setLeaderFlag(false)
			return
		}
	} else {
		if err := k.campaign(); err != nil {
			log.Errorf("campaign failed: %s", err)
			return
		}
	}

	if !k.initCompleted {
		k.firstInitWg.Done()
	}
}

func (k *Keeper) campaign() error {
	var ret []LeaderPayload
	if err := k.db.Table(k.leaderClsName).Find(&ret).Error; err != nil {
		return fmt.Errorf("find data failed: %w", err)
	}

	if len(ret) > 0 {
		if ret[0].WorkerKey == k.opt.Key {
			k.setLeaderFlag(true)
			return nil
		}
		if ret[0].UpdatedAt.Before(time.Now().Add(-1 * k.opt.UnhealthyTime)) {
			db := k.db.Table(k.leaderClsName).Where("id = ? and worker_key = ?", LeaderKey, ret[0].WorkerKey).Updates(map[string]any{
				"worker_key": k.opt.Key,
				"updated_at": time.Now(),
			})
			if db.Error != nil {
				return fmt.Errorf("update failed: %w", db.Error)

			}
			if db.RowsAffected > 0 {
				k.setLeaderFlag(true)
			}
		}
	}
	if len(ret) == 0 {
		err := k.db.Table(k.leaderClsName).Create(&LeaderPayload{
			ID:        LeaderKey,
			WorkerKey: k.opt.Key,
			UpdatedAt: time.Now(),
		}).Error
		if err != nil {
			log.Errorf("insert campaign rec failed: %s", err)
			return fmt.Errorf("insert failed: %w", err)
		}
		k.setLeaderFlag(true)
	}
	return nil
}

func (k *Keeper) continueLeader() error {

	ret := k.db.Table(k.leaderClsName).Where("id = ? and worker_key = ?", LeaderKey, k.opt.Key).Update("updated_at", time.Now())
	if ret.Error != nil {
		return fmt.Errorf("update failed: %w", ret.Error)
	}

	if ret.RowsAffected == 0 {
		return fmt.Errorf("re-elected failed")
	}
	return nil
}

func (k *Keeper) goHeartBeat() {
	timerCh := time.Tick(k.opt.UnhealthyTime / 2)
	closed := false
	for !closed {
		select {
		case <-k.closeCh:
			closed = true
		case <-timerCh:
			if err := k.heartBeat(); err != nil {
				log.Errorf("heart beat failed: %s", err)
				continue
			}
		}
		if !k.initCompleted {
			k.firstInitWg.Done()
		}
	}
	k.wg.Done()
}

func (k *Keeper) heartBeat() error {

	var ret Payload
	err := k.db.Table(k.heartbeatClsName).Where("id = ?", k.opt.Key).First(&ret).Error

	if err != nil && err != gorm.ErrRecordNotFound {
		return err
	}

	if err == gorm.ErrRecordNotFound {
		if err := k.db.Table(k.heartbeatClsName).Create(&Payload{
			WorkerKey: k.opt.Key,
			UpdatedAt: time.Now(),
		}).Error; err != nil {
			return err
		}

		return nil
	}

	err = k.db.Table(k.heartbeatClsName).Where("id = ?", k.opt.Key).Update("updated_at", time.Now()).Error
	if err != nil {
		return fmt.Errorf("update failed: %w", err)
	}
	return nil
}
