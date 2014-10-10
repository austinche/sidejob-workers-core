require 'spec_helper'

describe Workers::DelayedForwarder do
  before do
    @job = SideJob.queue('core', 'Workers::DelayedForwarder')
  end

  it 'suspends when no input' do
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'suspended'
  end

  it 'completes if delay provided but no input' do
    @job.input(:delay).write 10
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'completed'
  end

  it 'forwards one data packet only after delay' do
    now = Time.now
    allow(Time).to receive(:now) { now }
    @job.input(:delay).write 10
    @job.input(:in).write [1, 2]
    SideJob::Worker.drain_queue
    expect(@job.output(:out).size).to eq 0
    expect(@job.status).to eq 'queued' # should be scheduled
    allow(Time).to receive(:now) { now + 100 }
    @job.run # force immediate run
    SideJob::Worker.drain_queue
    expect(@job.output(:out).read).to eq [1, 2]
    expect(@job.status).to eq 'completed'
  end

  it 'forwards multiple packets in order each with own delay' do
    now = Time.now
    allow(Time).to receive(:now) { now }
    @job.input(:delay).write 10
    @job.input(:in).write 1
    SideJob::Worker.drain_queue
    allow(Time).to receive(:now) { now + 5 }
    @job.input(:in).write 2
    SideJob::Worker.drain_queue
    expect(@job.status).to eq 'queued' # should be scheduled
    expect(@job.output(:out).size).to eq 0
    allow(Time).to receive(:now) { now + 10 }
    @job.run # force immediate run
    SideJob::Worker.drain_queue
    expect(@job.output(:out).size).to eq 1
    expect(@job.output(:out).read).to eq 1
    expect(@job.status).to eq 'queued'
    allow(Time).to receive(:now) { now + 15 }
    @job.run # force immediate run
    SideJob::Worker.drain_queue
    expect(@job.output(:out).size).to eq 1
    expect(@job.output(:out).read).to eq 2
    expect(@job.status).to eq 'completed'
  end
end
