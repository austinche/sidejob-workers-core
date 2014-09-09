module Workers
  class Group
    include SideJob::Worker
    register('core', 'Workers::Group', {
        description: 'Groups input into an array',
        icon: 'group',
        inports: [
            { name: 'n', type: 'integer', description: 'Number of data to group' },
            { name: 'in', type: 'all', description: 'Input data' },
        ],
        outports: [
            { name: 'out', type: 'array', description: 'Output data' },
        ],
    })

    def perform
      n = get_config(:n)
      suspend unless n && n > 0

      data = get(:data) || []

      # read all data
      data = data.concat(input(:in).drain)
      # write out data
      outport = output(:out)
      while data.length >= n
        outport.write data.take(n)
        data = data.drop(n)
      end

      set(data: data)
      suspend if data.length != 0
    end
  end
end
