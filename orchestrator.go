package orchestrator

import (
	"sync"

	"git.resultys.com.br/lib/lower/collection/queue"
	"git.resultys.com.br/lib/lower/promise"
	"git.resultys.com.br/lib/lower/time"
	"git.resultys.com.br/prospecta/orchestrator/compute"
	"git.resultys.com.br/prospecta/orchestrator/manager"
)

type item struct {
	id       int
	callback func(*compute.Instance)
}

func (it item) GetID() int {
	return it.id
}

// Orchestrator struct
type Orchestrator struct {
	parallel     int
	totalRunning int
	manager      *manager.Manager
	fila         *queue.Queue
	mutex        *sync.Mutex
}

// New cria orchestrator
func New() *Orchestrator {
	o := &Orchestrator{}

	o.fila = queue.New()
	o.mutex = &sync.Mutex{}

	o.manager = manager.New()
	o.manager.OnRelease(o.processFila)

	return o
}

// Parallel informa quantidade de instancias que serao executadas em paralelo
// Valor de 0 indica que não haverá sequenciamento das chamadas
func (o *Orchestrator) Parallel(parallel int) *Orchestrator {
	o.parallel = parallel

	return o
}

// AddInstance adiciona instancia a bulk de instancias
func (o *Orchestrator) AddInstance(instance *compute.Instance) *Orchestrator {
	o.manager.AddInstance(instance)

	return o
}

// GetInstance retorna uma instancia aleatoria
func (o *Orchestrator) GetInstance(callback func(*compute.Instance)) *Orchestrator {
	callback(o.manager.NextInstance())

	return o
}

// BlockInstance bloqueia a instancia por 60 segundos
func (o *Orchestrator) BlockInstance(instance *compute.Instance) *Orchestrator {
	o.manager.BlockInstance(instance)

	time.Timeout(60, func() {
		o.manager.ReleaseInstance(instance)
	})

	return o
}

// AllocInstance aloca instancia para execução de uma tarefa
func (o *Orchestrator) AllocInstance(callback func(*compute.Instance)) *promise.Promise {
	promise := &promise.Promise{}

	o.mutex.Lock()

	if o.manager.IsBlockedInstances() {
		promise.Reject("todas as instancias estão bloqueadas")
		o.mutex.Unlock()
		return promise
	}

	if o.isFull() {
		o.fila.Push(item{callback: callback})
		o.mutex.Unlock()
		return promise
	}

	instance := o.manager.GetInstance()

	if instance == nil {
		o.fila.Push(item{callback: callback})
		o.mutex.Unlock()
		return promise
	}

	o.incRunning()
	o.mutex.Unlock()

	callback(instance)

	o.mutex.Lock()
	o.decRunning()
	o.mutex.Unlock()
	if instance.Status == compute.RUNNING {
		o.manager.ReleaseInstance(instance)
	}

	return promise
}

func (o *Orchestrator) isFull() bool {
	return o.totalRunning == o.parallel
}

func (o *Orchestrator) incRunning() {
	o.totalRunning++
}

func (o *Orchestrator) decRunning() {
	o.totalRunning--
}

func (o *Orchestrator) processFila(instance *compute.Instance) {
	if o.fila.IsEmpty() {
		return
	}

	item := o.fila.Pop().(item)
	o.AllocInstance(item.callback)
}
