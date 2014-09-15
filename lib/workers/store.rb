module Workers
  class Store
    include SideJob::Worker
    register('core', 'Workers::Store', {
        description: 'Store and retrieve data',
        icon: 'database',
        inports: [
            { name: 'store', type: 'all', description: 'Data to store' },
            { name: 'retrieve', type: 'bang', description: 'Sends stored data on every packet' },
        ],
        outports: [
            { name: 'data', type: 'all', description: 'Stored data' },
        ],
    })

    def perform
      # handle stores
      if input(:store).size > 0
        data = input(:store).drain.last
        set(data: data)
      else
        data = get(:data)
      end

      # handle retrieves
      input(:retrieve).drain.length.times do
        output(:data).write data
      end
    end
  end
end
