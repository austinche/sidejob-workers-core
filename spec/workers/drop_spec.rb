require 'spec_helper'

describe Workers::Drop do
  before do
    @job = SideJob.queue('core', 'Workers::Drop')
  end

  it 'completes on no input' do
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'completed'
  end

  it 'drains data' do
    @job.input(:in).write 1, 2, 3
    SideJob::Worker.drain_queue
    expect(@job.input(:in).size).to be 0
    expect(@job.status).to eq 'completed'
  end
end
