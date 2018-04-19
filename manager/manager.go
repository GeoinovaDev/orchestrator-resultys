package manager

import (
	"sync"

	"git.resultys.com.br/prospecta/orchestrator/compute"
)

// Manager struct
type Manager struct {
	mutex     *sync.Mutex
	instances []*compute.Instance
	onRelease func(*compute.Instance)
	seq       int
}

// New cria estrutura
func New() *Manager {
	manager := &Manager{
		instances: make([]*compute.Instance, 0),
		mutex:     &sync.Mutex{},
	}

	return manager
}

// NextInstance retorna a proxima instancia da sequencia
func (m *Manager) NextInstance() *compute.Instance {
	instancia := m.instances[m.seq%len(m.instances)]
	m.seq++
	return instancia
}

// AddInstance adiciona instancia ao manager
func (m *Manager) AddInstance(instance *compute.Instance) {
	m.mutex.Lock()
	defer m.mutex.Unlock()

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

// GetInstance retorna uma instancia disponivel para execução
// Return compute.Instance
func (m *Manager) GetInstance() *compute.Instance {
	m.mutex.Lock()
	defer m.mutex.Unlock()

	var selected *compute.Instance

	for i := 0; i < len(m.instances); i++ {
		instance := m.instances[i]
		if instance.Status == compute.READY {
			instance.Status = compute.RUNNING
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

// ReleaseInstance libera instancia para processamento
func (m *Manager) ReleaseInstance(instance *compute.Instance) {
	m.mutex.Lock()
	instance.Status = compute.READY
	m.mutex.Unlock()

	if m.onRelease != nil {
		m.onRelease(instance)
	}
}

// BlockInstance marca instancia como bloqueada
func (m *Manager) BlockInstance(instance *compute.Instance) {
	m.mutex.Lock()
	instance.Status = compute.BLOCKED
	m.mutex.Unlock()
}

// OnRelease dispara callback ao liberar uma instancia
func (m *Manager) OnRelease(callback func(*compute.Instance)) {
	m.onRelease = callback
}
