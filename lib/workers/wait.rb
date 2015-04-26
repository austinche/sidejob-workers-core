module Workers
  class Wait
    include SideJob::Worker
    register(
        description: 'Waits for a trigger before sending data from in to out',
        icon: 'step-forward',
        inports: {
            trigger: { type: 'all', description: 'Trigger' },
            in: { type: 'all', description: 'Input data' },
        },
        outports: {
            out: { type: 'all', description: 'Output data' },
        },
    )

    def perform
      for_inputs(:trigger, :in) do |trigger, input|
        output(:out).write input
      end
    end
  end
end
