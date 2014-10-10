module Workers
  class ObjectMerge
    include SideJob::Worker
    register('core', 'Workers::ObjectMerge', {
        description: 'Merges multiple input JSON objects into one.
Input ports are dynamic and merged in alphabetical order by port name.
Waits for one packet on every known port. A known port is any port that has previously received data.
Any null packets are dropped. Therefore, send an initial null to any port that may have slow arriving data.',
        icon: 'list-ul',
        inports: [],
        outports: [
            { name: 'out', type: 'object', description: 'Merged object' },
        ],
    })

    def perform
      current = get(:current) || {} # port -> current object data
      loop do
        any = false
        all = true

        inports.each do |port|
          while port.size > 0 && current[port.name].nil?
            current[port.name] = port.read
          end
          if current[port.name]
            any = true
          else
            all = false
          end
        end

        set({current: current})

        # complete if no inputs, suspend if partial inputs
        return unless any
        suspend unless all

        # now merge all data and generate output
        result = inports.sort_by(&:name).each_with_object({}) do |port, result|
          result.merge!(current[port.name])
          current.delete(port.name)
        end
        output(:out).write result

        set({current: current})
      end
    end
  end
end
