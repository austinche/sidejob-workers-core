require 'spec_helper'

describe Workers::Wait do
  before do
    @job = SideJob.queue('core', 'Workers::Wait')
  end

  it 'completes on no input' do
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'completed'
  end

  it 'suspends if data on in port' do
    @job.input(:in).write 1
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'suspended'
    expect(@job.input(:in).size).to be 1
  end

  it 'completes and forwards in port if ready' do
    @job.input(:in).write 1
    @job.input(:ready).write 1
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).read).to eq 1
  end
end
