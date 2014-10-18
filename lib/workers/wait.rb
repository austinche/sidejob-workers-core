module Workers
  class Wait
    include SideJob::Worker
    register(
        description: 'Waits for data on input ports and sends selected to the same named output ports',
        icon: 'step-forward',
        inports: {
            '*' => { type: 'all', description: 'Input data' },
        },
        outports: {
            '*' => { type: 'all', description: 'Output data' },
        },
    )

    def perform
      inports_names = inports.map(&:name)
      out = outports.each_with_object({}) {|port, hash| hash[port.name] = port}
      for_inputs(*inports_names) do |*data|
        data.each_with_index do |x, i|
          port = inports_names[i]
          out[port].write x if out[port]
        end
      end
    end
  end
end
