package mysql

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	"github.com/igxm/fastflow/pkg/entity"
	"github.com/igxm/fastflow/pkg/event"
	"github.com/igxm/fastflow/pkg/mod"
	"github.com/igxm/fastflow/pkg/utils"
	"github.com/igxm/fastflow/pkg/utils/data"
	"github.com/shiningrush/goevent"
	"go.mongodb.org/mongo-driver/bson"
	"gorm.io/driver/mysql"
	"gorm.io/gorm"
)

type StoreOption struct {
	Db *gorm.DB
	// mysql connection settings
	Dsn string

	// timeout access mysql timeout.default 5s
	Timeout time.Duration
	// table the prefix
	Prefix string
}

type Store struct {
	opt            *StoreOption
	dagClsName     string
	dagInsClsName  string
	taskInsClsName string

	db *gorm.DB
}

// NewStore
func NewStore(option *StoreOption) *Store {
	return &Store{
		opt: option,
	}
}

// Init Store
func (s *Store) Init() error {
	if err := s.readOpt(); err != nil {
		return err
	}

	if s.opt.Db == nil {

		mysqlConfig := mysql.Config{
			DSN:                       s.opt.Dsn, // DSN data source name
			DisableDatetimePrecision:  true,      // 禁用 datetime 精度，MySQL 5.6 之前的数据库不支持
			SkipInitializeWithVersion: false,     // 根据版本自动配置
		}

		db, err := gorm.Open(mysql.New(mysqlConfig), &gorm.Config{
			DisableForeignKeyConstraintWhenMigrating: true,
		})
		if err != nil {
			return err
		}
		s.db = db
	} else {
		s.db = s.opt.Db
	}

	return nil
}

func (s *Store) readOpt() error {
	if s.opt.Db == nil {
		if s.opt.Dsn == "" {
			return fmt.Errorf("Dsn string cannot be empty")
		}
	}

	if s.opt.Timeout == 0 {
		s.opt.Timeout = 5 * time.Second
	}
	s.dagClsName = "dag"
	s.dagInsClsName = "dag_instance"
	s.taskInsClsName = "task_instance"
	if s.opt.Prefix != "" {
		s.dagClsName = fmt.Sprintf("%s_%s", s.opt.Prefix, s.dagClsName)
		s.dagInsClsName = fmt.Sprintf("%s_%s", s.opt.Prefix, s.dagInsClsName)
		s.taskInsClsName = fmt.Sprintf("%s_%s", s.opt.Prefix, s.taskInsClsName)
	}

	return nil
}

// Close component when we not use it anymore
func (s *Store) Close() {
	return
}

// CreateDag
func (s *Store) CreateDag(dag *entity.Dag) error {
	// check task's connection
	_, err := mod.BuildRootNode(mod.MapTasksToGetter(dag.Tasks))
	if err != nil {
		return err
	}

	return s.genericCreate(dag, s.dagClsName)
}

// CreateDagIns
func (s *Store) CreateDagIns(dagIns *entity.DagInstance) error {
	return s.genericCreate(dagIns, s.dagInsClsName)
}

// CreateTaskIns
func (s *Store) CreateTaskIns(taskIns *entity.TaskInstance) error {
	return s.genericCreate(taskIns, s.taskInsClsName)
}

func (s *Store) genericCreate(input entity.BaseInfoGetter, clsName string) error {
	baseInfo := input.GetBaseInfo()
	baseInfo.Initial()

	ctx, cancel := context.WithTimeout(context.TODO(), s.opt.Timeout)
	defer cancel()

	if err := s.db.WithContext(ctx).Table(clsName).Create(input).Error; err != nil {
		return fmt.Errorf("insert instance failed: %w", err)
	}

	return nil
}

// BatchCreatTaskIns
func (s *Store) BatchCreatTaskIns(taskIns []*entity.TaskInstance) error {
	ctx, cancel := context.WithTimeout(context.TODO(), s.opt.Timeout)
	defer cancel()

	for i := range taskIns {
		taskIns[i].Initial()
		if err := s.db.WithContext(ctx).Table(s.taskInsClsName).Create(taskIns[i]).Error; err != nil {
			return fmt.Errorf("insert task instance failed: %w", err)
		}
	}
	return nil
}

// PatchTaskIns
func (s *Store) PatchTaskIns(taskIns *entity.TaskInstance) error {
	if taskIns.ID == "" {
		return fmt.Errorf("id cannot be empty")
	}

	update := map[string]any{
		"updated_at": time.Now().Unix(),
	}
	if taskIns.Status != "" {
		update["status"] = taskIns.Status
	}
	if taskIns.Reason != "" {
		update["reason"] = taskIns.Reason
	}
	if len(taskIns.Traces) > 0 {
		update["traces"] = taskIns.Traces
	}

	if err := s.db.Table(s.taskInsClsName).Where("id = ?", taskIns.ID).Updates(update).Error; err != nil {
		return fmt.Errorf("patch task instance failed: %w", err)
	}

	return nil
}

// PatchDagIns
func (s *Store) PatchDagIns(dagIns *entity.DagInstance, mustsPatchFields ...string) error {
	update := map[string]any{
		"updated_at": time.Now().Unix(),
	}

	if dagIns.ShareData != nil {
		update["share_data"] = dagIns.ShareData
	}
	if dagIns.Status != "" {
		update["status"] = dagIns.Status
	}
	if utils.StringsContain(mustsPatchFields, "Cmd") || dagIns.Cmd != nil {
		update["cmd"] = dagIns.Cmd
	}
	if dagIns.Worker != "" {
		update["worker"] = dagIns.Worker
	}
	if utils.StringsContain(mustsPatchFields, "Reason") || dagIns.Reason != "" {
		update["reason"] = dagIns.Reason
	}

	if err := s.db.Table(s.dagInsClsName).Where("id = ?", dagIns.ID).Updates(update).Error; err != nil {
		return fmt.Errorf("patch dag instance failed: %w", err)
	}

	goevent.Publish(&event.DagInstancePatched{
		Payload:         dagIns,
		MustPatchFields: mustsPatchFields,
	})
	return nil
}

// UpdateDag
func (s *Store) UpdateDag(dag *entity.Dag) error {
	// check task's connection
	_, err := mod.BuildRootNode(mod.MapTasksToGetter(dag.Tasks))
	if err != nil {
		return err
	}

	values := make(map[string]any)
	values["updated_at"] = time.Now().Unix()
	values["name"] = dag.Name
	values["desc"] = dag.Desc
	values["cron"] = dag.Cron
	values["vars"] = dag.Vars
	values["status"] = dag.Status
	values["tasks"] = dag.Tasks

	return s.genericUpdate(dag.ID, values, s.dagClsName)
}

// UpdateDagIns
func (s *Store) UpdateDagIns(dagIns *entity.DagInstance) error {
	values := make(map[string]any)
	values["updated_at"] = time.Now().Unix()
	values["dag_id"] = dagIns.DagID
	values["trigger"] = dagIns.Trigger
	values["worker"] = dagIns.Worker
	values["vars"] = dagIns.Vars
	values["share_data"] = dagIns.ShareData
	values["status"] = dagIns.Status
	values["reason"] = dagIns.Reason
	values["cmd"] = dagIns.Cmd

	if err := s.genericUpdate(dagIns.ID, values, s.dagInsClsName); err != nil {
		return err
	}

	goevent.Publish(&event.DagInstanceUpdated{Payload: dagIns})
	return nil
}

// UpdateTaskIns
func (s *Store) UpdateTaskIns(taskIns *entity.TaskInstance) error {
	values := make(map[string]any)
	values["updated_at"] = time.Now().Unix()
	values["task_id"] = taskIns.TaskID
	values["dag_ins_id"] = taskIns.DagInsID
	values["name"] = taskIns.Name
	values["depend_on"] = taskIns.DependOn
	values["action_name"] = taskIns.ActionName
	values["timeout_secs"] = taskIns.TimeoutSecs
	values["params"] = taskIns.Params
	values["traces"] = taskIns.Traces
	values["status"] = taskIns.Status
	values["reason"] = taskIns.Reason
	values["pre_checks"] = taskIns.PreChecks

	return s.genericUpdate(taskIns.ID, values, s.taskInsClsName)
}

// genericUpdate
func (s *Store) genericUpdate(id string, values map[string]any, clsName string) error {

	err := s.db.Table(clsName).Where("id = ?", id).Updates(values).Error
	if err != nil {
		return fmt.Errorf("update dag instance failed: %w", err)
	}

	return nil
}

// BatchUpdateDagIns
func (s *Store) BatchUpdateDagIns(dagIns []*entity.DagInstance) error {

	errChan := make(chan error)
	defer close(errChan)

	errs := &data.Errors{}
	go func() {
		for err := range errChan {
			errs.Append(err)
		}
	}()

	wg := sync.WaitGroup{}
	for i := range dagIns {
		wg.Add(1)
		go func(dag *entity.DagInstance, ch chan error) {
			if err := s.UpdateDagIns(dag); err != nil {
				errChan <- fmt.Errorf("batch update dag instance failed: %w", err)
			}
			wg.Done()
		}(dagIns[i], errChan)
	}
	wg.Wait()
	return nil
}

// BatchUpdateTaskIns
func (s *Store) BatchUpdateTaskIns(taskIns []*entity.TaskInstance) error {

	for i := range taskIns {
		if err := s.UpdateTaskIns(taskIns[i]); err != nil {
			return fmt.Errorf("batch update task instance failed: %w", err)
		}
	}
	return nil
}

// GetTaskIns
func (s *Store) GetTaskIns(taskInsId string) (*entity.TaskInstance, error) {
	ret := new(entity.TaskInstance)
	if err := s.genericGet(s.taskInsClsName, taskInsId, ret); err != nil {
		return nil, err
	}

	return ret, nil
}

// GetDag
func (s *Store) GetDag(dagId string) (*entity.Dag, error) {
	ret := new(entity.Dag)
	if err := s.genericGet(s.dagClsName, dagId, ret); err != nil {
		return nil, err
	}

	return ret, nil
}

// GetDagInstance
func (s *Store) GetDagInstance(dagInsId string) (*entity.DagInstance, error) {
	ret := new(entity.DagInstance)
	if err := s.genericGet(s.dagInsClsName, dagInsId, ret); err != nil {
		return nil, err
	}

	return ret, nil
}

func (s *Store) genericGet(clsName, id string, ret interface{}) error {

	if err := s.db.Table(clsName).Where("id = ?", id).Scan(ret).Error; err != nil {
		return fmt.Errorf("get dag instance failed: %w", err)
	}

	return nil
}

// ListDag
func (s *Store) ListDag(input *mod.ListDagInput) ([]*entity.Dag, error) {

	var ret []*entity.Dag
	err := s.db.Table(s.dagClsName).Find(&ret).Error
	if err != nil {
		return nil, err
	}
	return ret, nil
}

// ListDagInstance
func (s *Store) ListDagInstance(input *mod.ListDagInstanceInput) ([]*entity.DagInstance, error) {
	db := s.db.Table(s.dagInsClsName)
	if len(input.Status) > 0 {
		db = db.Where("status in ?", input.Status)
	}

	if input.Worker != "" {
		db = db.Where("worker = ?", input.Worker)
	}

	if input.UpdatedEnd > 0 {
		db = db.Where("updated_at <= ?", input.UpdatedEnd)
	}
	if input.HasCmd {
		db = db.Where("cmd is not null")
	}

	if input.Limit > 0 {
		offset := input.Offset
		if offset > 0 {
			offset = (offset - 1) * input.Limit

		}
		db = db.Offset(int(offset)).Limit(int(input.Limit))
	}

	var ret []*entity.DagInstance
	err := db.Find(&ret).Error
	if err != nil {
		return nil, err
	}
	return ret, nil
}

// ListTaskInstance
func (s *Store) ListTaskInstance(input *mod.ListTaskInstanceInput) ([]*entity.TaskInstance, error) {
	db := s.db.Table(s.taskInsClsName)

	if len(input.IDs) > 0 {
		db = db.Where("id in ?", input.IDs)
	}

	if len(input.Status) > 0 {
		db = db.Where("status in ?", input.Status)
	}

	if input.Expired {
		db = db.Where("updated_at <= (unix_timestamp() - 5) - timeout_secs")
	}

	if input.DagInsID != "" {
		db = db.Where("dag_ins_id = ?", input.DagInsID)
	}

	var ret []*entity.TaskInstance

	err := db.Find(&ret).Error
	if err != nil {
		return nil, err
	}
	return ret, nil
}

// BatchDeleteDag
func (s *Store) BatchDeleteDag(ids []string) error {
	return s.genericBatchDelete(ids, s.dagClsName)
}

// BatchDeleteDagIns
func (s *Store) BatchDeleteDagIns(ids []string) error {
	return s.genericBatchDelete(ids, s.dagInsClsName)
}

// BatchDeleteTaskIns
func (s *Store) BatchDeleteTaskIns(ids []string) error {
	return s.genericBatchDelete(ids, s.taskInsClsName)
}

func (s *Store) genericBatchDelete(ids []string, clsName string) error {

	err := s.db.Exec(fmt.Sprintf("delete from %s where id in ?", clsName), ids).Error

	if err != nil {
		return fmt.Errorf("delete failed: %w", err)
	}

	return nil
}

// Marshal
func (s *Store) Marshal(obj interface{}) ([]byte, error) {
	return json.Marshal(obj)
}

// Unmarshal
func (s *Store) Unmarshal(bytes []byte, ptr interface{}) error {
	return bson.Unmarshal(bytes, ptr)
}
