module Workers
  class SetDefault
    include SideJob::Worker
    register(
        description: 'Sets the default value to the last value received or input default value',
        icon: 'clipboard',
        inports: {
            in: { type: 'all', description: 'Input data' },
        },
        outports: {
            out: { type: 'all', description: 'Output data' },
        },
    )

    def perform
      entries = input(:in).entries
      if input(:in).default?
        output(:out).default = input(:in).default
      elsif entries.length > 0
        output(:out).default = entries.last
      end
    end
  end
end
