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
      suspend # suspends for the external widget to handle
    end
  end
end
