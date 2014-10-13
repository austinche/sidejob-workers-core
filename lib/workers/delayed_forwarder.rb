module Workers
  class DelayedForwarder
    include SideJob::Worker
    register(
        description: 'Forwards all data on input port to output port after a delay',
        icon: 'step-forward',
        inports: {
            delay: { mode: :memory, type: 'integer', description: 'Number of seconds to delay sending data' },
            in: { type: 'all', description: 'Input data' },
        },
        outports: {
            out: { type: 'all', description: 'Output data' },
        },
    )

    def perform
      queue = get(:queue) || []
      begin
        for_inputs(:delay, :in) do |delay, input|
          # maintain queue sorted by increasing time to actually send data
          time = Time.now.to_f + delay
          index = queue.index {|item| item['time'] > time} || -1
          queue.insert index, { 'data' => input, 'time' => time }
        end
      ensure
        while queue.length > 0
          break if queue[0]['time'] > Time.now.to_f
          data = queue.shift
          output(:out).write data['data']
        end

        set(queue: queue)
        run(at: queue[0]['time']) if queue.length > 0
      end
    end
  end
end
