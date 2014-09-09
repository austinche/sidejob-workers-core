module Workers
  class Wait2
    include SideJob::Worker
    register('core', 'Workers::Wait2', {
        description: 'Waits for inputs on two ports',
        icon: 'step-forward',
        inports: [
            { name: 'in1', type: 'all', description: 'Input data 1' },
            { name: 'in2', type: 'all', description: 'Input data 2' },
        ],
        outports: [
            { name: 'out1', type: 'all', description: 'Output data 1' },
            { name: 'out2', type: 'all', description: 'Output data 2' },
        ],
    })

    def perform
      in1 = input(:in1)
      in2 = input(:in2)
      return unless in1.size > 0 || in2.size > 0

      while in1.size > 0 && in2.size > 0
        data1 = in1.read
        data2 = in2.read
        output(:out1).write data1
        output(:out2).write data2
      end

      suspend if in1.size != 0 || in2.size != 0
    end
  end
end
