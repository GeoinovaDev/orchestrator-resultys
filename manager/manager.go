package manager

import (
	"sync"

	"github.com/GeoinovaDev/orchestrator-resultys/compute"
)

// Manager struct
type Manager struct {
	mutex     *sync.Mutex
	instances []*compute.Instance
	seq       int
	parallel  int
}

// New cria estrutura
func New() *Manager {
	manager := &Manager{
		mutex: &sync.Mutex{},
	}

	return manager
}

// Lock trava o manager
func (m *Manager) Lock() *Manager {
	m.mutex.Lock()

	return m
}

// Unlock destrava o manager
func (m *Manager) Unlock() *Manager {
	m.mutex.Unlock()

	return m
}

// Parallel determina o numero de instancias que poderam ser alocadas paralelamente
func (m *Manager) Parallel(parallel int) *Manager {
	m.parallel = parallel

	return m
}

// NextInstance retorna a proxima instancia da sequencia
func (m *Manager) NextInstance() *compute.Instance {
	instancia := m.instances[m.seq%len(m.instances)]
	m.seq++
	return instancia
}

// AddInstance adiciona instancia ao manager
func (m *Manager) AddInstance(instance *compute.Instance) {
	if m.ExistInstance(instance) {
		return
	}

	m.instances = append(m.instances, instance)
}

// ExistInstance verifica se existe instancia no manager
// Return boolean
func (m *Manager) ExistInstance(instance *compute.Instance) bool {
	b := false

	for _, inst := range m.instances {
		if inst.IP == instance.IP {
			b = true
			break
		}
	}

	return b
}

// GetInstance retorna instance pelo stats
func (m *Manager) GetInstance(status int) *compute.Instance {
	var selected *compute.Instance

	for i := 0; i < len(m.instances); i++ {
		instance := m.instances[i]

		if status == compute.RUNNING {
			if instance.Status == status && instance.Running < m.parallel {
				selected = instance
				break
			}
		} else if instance.Status == status {
			selected = instance
			break
		}
	}

	return selected
}

// IsBlockedInstances verifica se todas as instances estão bloqueadas
// Retorna boolean
func (m *Manager) IsBlockedInstances() bool {
	var blocked = true

	for i := 0; i < len(m.instances); i++ {
		instance := m.instances[i]
		if instance.Status != compute.BLOCKED {
			blocked = false
			break
		}
	}

	return blocked
}

// TotalInstance calcula o total de instance pelo status
func (m *Manager) TotalInstance(status int) int {
	total := 0

	for i := 0; i < len(m.instances); i++ {
		instance := m.instances[i]
		if instance.Status == status {
			total++
		}
	}

	return total
}

// AllocInstance aloca instancia para processamento
func (m *Manager) AllocInstance(instance *compute.Instance) *Manager {
	instance.Status = compute.RUNNING
	instance.Running++

	return m
}

// ReleaseInstance libera instancia para processamento
func (m *Manager) ReleaseInstance(instance *compute.Instance) *Manager {
	instance.Running--
	if instance.Running == 0 && instance.Status == compute.RUNNING {
		instance.Status = compute.READY
	}

	return m
}

// BlockInstance marca instancia como bloqueada
func (m *Manager) BlockInstance(instance *compute.Instance) *Manager {
	m.Lock()
	instance.Status = compute.BLOCKED
	m.Unlock()

	return m
}

// UnBlockInstance desbloqueia instancia
func (m *Manager) UnBlockInstance(instance *compute.Instance) *Manager {
	m.Lock()
	instance.Running = 0
	instance.Status = compute.READY
	m.Unlock()

	return m
}
