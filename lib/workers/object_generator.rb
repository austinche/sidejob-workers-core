module Workers
  class ObjectGenerator
    include SideJob::Worker
    register('core', 'Workers::ObjectGenerator', {
        description: 'Generates JSON objects by merging multiple inputs',
        icon: 'list-ul',
        inports: [
            { name: 'config', type: 'array', description: 'Array of object specifications to be merged in order. Each object has keys: port (required), key, use_recent, from_array' },
            { name: 'in1', type: 'all' },
            { name: 'in2', type: 'all' },
            { name: 'in3', type: 'all' },
            { name: 'in4', type: 'all' },
        ],
        outports: [
            { name: 'out', type: 'object', description: 'Output objects' },
        ],
    })

    def perform
      config = get_config(:config)
      suspend unless config

      current = get_config(:current) || {} # port -> current object data
      loop do
        any = false
        all = true

        # load in current values
        config.each do |item|
          port = item['port']
          raise 'Missing port in config' unless port

          if item['use_recent']
            # only the most recent data is used and it is saved for reuse
            data = hash_from_input(input(port).drain.last, item)
            current[port] = data if ! data.nil?
          else
            # default is to read data one at a time
            if current[port].nil?
              current[port] = hash_from_input(input(port).read, item)
            end
            any = true if current[port]
          end

          all = false unless current[port]
        end

        set({current: current})

        # complete if no inputs, suspend if partial inputs
        return unless any
        suspend unless all

        # now merge all data and generate output
        result = config.each_with_object({}) do |item, result|
          port = item['port']
          result.merge!(current[port])
          current.delete(port) unless item['use_recent']
        end
        output(:out).write result

        set({current: current})
      end
    end

    private

    def hash_from_input(data, item)
      return nil if data.nil?

      if item['from_array']
        data = data.to_h
      end

      key = item['key']
      if key
        x = {}
        x[key] = data
        x
      else
        data
      end
    end
  end
end
