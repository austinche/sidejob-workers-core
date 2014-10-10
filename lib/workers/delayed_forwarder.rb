module Workers
  class DelayedForwarder
    include SideJob::Worker
    register('core', 'Workers::DelayedForwarder', {
        description: 'Forwards all data on input port to output port after a delay',
        icon: 'step-forward',
        inports: [
            { name: 'delay', type: 'integer', description: 'Number of seconds to delay every data packet' },
            { name: 'in', type: 'all', description: 'Input data' },
        ],
        outports: [
            { name: 'out', type: 'all', description: 'Output data' },
        ],
    })

    def perform
      delay = get_config(:delay)
      suspend unless delay

      queue = get(:queue) || []

      input(:in).each do |data|
        queue << { 'data' => data, 'time' => Time.now.to_f }
      end

      cutoff = (Time.now - delay).to_f
      # queue is sorted by increasing queued time
      while queue.length > 0
        break if queue[0]['time'] > cutoff
        data = queue.shift
        output(:out).write data['data']
      end

      set(queue: queue)

      run(at: queue[0]['time'] + delay) if queue.length > 0
    end
  end
end
