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
    @job.input(:in).write 1, 2
    @job.input(:ready).write 1
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).drain).to eq [1, 2]
  end

  it 'can reset port' do
    @job.input(:in).write 1
    @job.input(:ready).write 1
    SideJob::Worker.drain_queue
    @job.input(:reset).write 1
    @job.input(:in).write 2
    expect(@job.output(:out).drain).to eq [1]
  end

  it 'can resend after reset' do
    @job.input(:in).write 1, 2
    @job.input(:reset).write 1
    SideJob::Worker.drain_queue
    expect(@job.output(:out).drain).to eq []
    @job.input(:ready).write 1
    SideJob::Worker.drain_queue
    expect(@job.output(:out).drain).to eq [1, 2]
  end
end
