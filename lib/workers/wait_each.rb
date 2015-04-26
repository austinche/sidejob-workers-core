module Workers
  class WaitEach
    include SideJob::Worker
    register(
        description: 'Waits for every element in an array before sending it out',
        icon: 'step-forward',
        inports: {
            each: { type: 'all', description: 'Each array element in any order' },
            in: { type: 'array', description: 'Input array' },
        },
        outports: {
            out: { type: 'array', description: 'Output array' },
        },
    )

    def perform
      array = get(:array)
      received = get(:received) || []
      loop do
        if ! array
          array = input(:in).read
          return if array == SideJob::Port::None
          set({array: array})
        end

        if input(:each).size > 0
          received.concat(input(:each).entries)
          set({received: received})
        end

        suspend unless received.length >= array.length # simple check that we may possibly have received everything

        # check if every element in array is in received
        # to handle duplicates, we can't just use the Array '- operator
        array.each do |elem|
          x = received.index(elem)
          suspend unless x
          received.delete_at(x)
        end

        output(:out).write array
        set({received: received})
        unset('array')
        array = nil
      end
    end
  end
end
