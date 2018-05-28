package orchestrator

import (
	"git.resultys.com.br/lib/lower/collection/queue"
	"git.resultys.com.br/lib/lower/promise"
	"git.resultys.com.br/lib/lower/time"
	"git.resultys.com.br/motor/orchestrator/compute"
	"git.resultys.com.br/motor/orchestrator/manager"
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
	manager      *manager.Manager
	fila         *queue.Queue
	timeoutBlock int
}

// New cria orchestrator
func New() *Orchestrator {
	o := &Orchestrator{}

	o.timeoutBlock = 20 * 60
	o.fila = queue.New()
	o.manager = manager.New()

	return o
}

// TimeoutBlock seta o tempo de tempo que instancia permanece bloqueada
func (o *Orchestrator) TimeoutBlock(segundos int) *Orchestrator {
	o.timeoutBlock = segundos

	return o
}

// ParallelInstance informa quantidade de instancias que poderao ser alocadas em paralelo
// Valor de 0 indica que não haverá sequenciamento das chamadas
func (o *Orchestrator) ParallelInstance(parallel int) *Orchestrator {
	o.parallel = parallel

	return o
}

// ParallelRequest informa quantidade de requests que serao executadas em paralelo
// Valor de 0 indica que não haverá sequenciamento das chamadas
func (o *Orchestrator) ParallelRequest(parallel int) *Orchestrator {
	o.manager.Parallel(parallel)

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

	time.Timeout(o.timeoutBlock, func() {
		o.manager.UnBlockInstance(instance)
	})

	return o
}

// AllocInstance aloca instancia para execução de uma tarefa
func (o *Orchestrator) AllocInstance(callback func(*compute.Instance)) *promise.Promise {
	var instance *compute.Instance
	promise := promise.New()

	o.manager.Lock()

	if o.manager.IsBlockedInstances() {
		promise.Reject("todas as instancias estão bloqueadas")
		o.manager.Unlock()
		return promise
	}

	if o.isFull() {
		instance = o.manager.GetInstance(compute.RUNNING)
	} else {
		instance = o.manager.GetInstance(compute.READY)
	}

	if instance == nil {
		o.fila.Push(item{callback: callback})
		o.manager.Unlock()
		return promise
	}

	o.manager.AllocInstance(instance)
	o.manager.Unlock()

	callback(instance)

	o.manager.Lock()
	o.manager.ReleaseInstance(instance)
	o.manager.Unlock()

	o.processFila()

	return promise
}

func (o *Orchestrator) isFull() bool {
	return o.parallel == o.manager.TotalInstance(compute.RUNNING)
}

func (o *Orchestrator) processFila() {
	if o.fila.IsEmpty() {
		return
	}

	item := o.fila.Pop().(item)
	o.AllocInstance(item.callback)
}
