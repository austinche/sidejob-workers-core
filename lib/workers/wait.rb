module Workers
  class Wait
    include SideJob::Worker
    register('core', 'Workers::Wait', {
        description: 'Waits for ready signal before forwarding input',
        icon: 'step-forward',
        inports: [
            { name: 'ready', type: 'bang', description: 'Forward one packet from in on any ready packet' },
            { name: 'in', type: 'all', description: 'Input data' },
        ],
        outports: [
            { name: 'out', type: 'all', description: 'Output data' },
        ],
    })

    def perform
      inport = input(:in)
      ready = input(:ready)
      outport = output(:out)
      suspend if ready.size == 0 && inport.size > 0
      ready.size.times do
        break if inport.size == 0
        ready.read
        outport.write inport.read
      end
    end
  end
end
