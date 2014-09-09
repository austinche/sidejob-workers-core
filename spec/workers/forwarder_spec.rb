require 'spec_helper'

describe Workers::Forwarder do
  before do
    @job = SideJob.queue('core', 'Workers::Forwarder')
  end

  it 'completes when no input' do
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'completed'
  end

  it 'forwards one data packet' do
    data = {'test' => 123}
    @job.input(:in).write(data)
    SideJob::Worker.drain_queue
    expect(@job.output(:out).read).to eq data
    expect(@job.status).to eq 'completed'
  end

  it 'forwards multiple packets in order' do
    data = {'test' => 123}
    @job.input(:in).write(data)
    data2 = {'test2' => 456}
    @job.input(:in).write(data2)
    SideJob::Worker.drain_queue
    expect(@job.output(:out).read).to eq data
    expect(@job.output(:out).read).to eq data2
    expect(@job.status).to eq 'completed'
  end
end
