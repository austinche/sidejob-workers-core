module Workers
  class Group
    include SideJob::Worker
    register(
        description: 'Groups input into an array',
        icon: 'group',
        inports: {
            n: { mode: :memory, type: 'integer', description: 'Number of data to group' },
            in: { type: 'all', description: 'Input data' },
        },
        outports: {
           out: { type: 'array', description: 'Output data' },
        },
    )

    def perform
      raise 'Port in cannot be a memory port' if input(:in).mode == :memory

      loop do
        n = get(:n)
        if ! n
          suspend unless input(:n).data?
          n = input(:n).read
          raise 'n must be > 0' unless n > 0
          set({n: n})
        end

        return unless input(:in).data?
        suspend if input(:in).size < n

        output(:out).write input(:in).take(n)
      end
    end
  end
end
