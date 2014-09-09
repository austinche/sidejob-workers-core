require 'spec_helper'

describe Workers::Wait2 do
  before do
    @job = SideJob.queue('core', 'Workers::Wait2')
  end

  it 'completes on no input' do
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'completed'
  end

  it 'suspends if data on only one port' do
    @job.input(:in1).write 1
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'suspended'
    expect(@job.input(:in1).size).to be 1
  end

  it 'completes if data on both ports' do
    @job.input(:in1).write 1
    @job.input(:in2).write 2
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out1).read).to eq 1
    expect(@job.output(:out2).read).to eq 2
  end
end
