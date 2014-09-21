module Workers
  class Wait
    include SideJob::Worker
    register('core', 'Workers::Wait', {
        description: 'Waits for ready signal before forwarding input',
        icon: 'step-forward',
        inports: [
            { name: 'ready', type: 'bang', description: 'Waits for any packet before forwarding packets' },
            { name: 'reset', type: 'bang', description: 'After a reset signal, waits for another ready signal' },
            { name: 'in', type: 'all', description: 'Input data' },
        ],
        outports: [
            { name: 'out', type: 'all', description: 'Output data' },
        ],
    })

    def perform
      ready = input(:ready)
      reset = input(:reset)
      inport = input(:in)
      outport = output(:out)

      return unless inport.size > 0

      # one reset packet cancels one ready packet
      while reset.size > 0
        unset(:ready) and suspend if ready.size == 0
        ready.read
        reset.read
      end

      if ! get(:ready)
        suspend unless ready.size > 0
        ready.read
        set({ready: true})
      end
      outport.write *inport.drain
    end
  end
end
