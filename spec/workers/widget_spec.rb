require 'spec_helper'

describe Workers::Widget do
  before do
    @job = SideJob.queue('core', 'Workers::Widget')
  end

  it 'sets status to user' do
    @job.run_inline
    expect(@job.status).to eq 'user'
  end

  it 'publishes a message via redis' do
    ENV['PUBSUB_HOST'] = 'localhost'
    redis = Redis.new(host: 'localhost')
    Timeout.timeout(5) do
      redis.subscribe("Widget:job/#{@job.id}") do |on|
        on.subscribe do
          @job.run_inline(args: ['MyWidget'])
        end
        on.message do |channel, message|
          expect(JSON.parse(message)).to eq({'event' => 'run', 'name' => 'MyWidget'})
          redis.unsubscribe
        end
      end
    end
  end
end
