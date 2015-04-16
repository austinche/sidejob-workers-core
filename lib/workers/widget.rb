# A widget represents a separate application or UI element that requires user input
module Workers
  class Widget
    include SideJob::Worker
    register(
        inports: {
            '*' => { type: 'all', description: 'Widget inport' },
        },
        outports: {
            '*' => { type: 'all', description: 'Widget outport' },
        },
    )

    def perform(widget_name=nil)
      # Set status to a custom one and publish a message to potentially notify any listeners that
      # this widget needs to be run for this job
      self.status = 'user'
      if ENV['PUBSUB_HOST']
        Redis.new(host: ENV['PUBSUB_HOST']).publish("Widget:job/#{id}", {event: 'run', name: widget_name}.to_json)
      end
    end
  end
end
