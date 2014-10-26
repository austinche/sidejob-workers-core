require 'spec_helper'

describe Workers::Delay do
  before do
    @job = SideJob.queue('core', 'Workers::Delay')
  end

  it 'forwards one data packet only after delay' do
    now = Time.now
    allow(Time).to receive(:now) { now }
    @job.input(:delay).write 10
    @job.input(:in).write [1, 2]
    @job.run_inline
    expect(@job.output(:out).size).to eq 0
    expect(@job.status).to eq 'queued' # should be scheduled
    allow(Time).to receive(:now) { now + 100 }
    @job.run_inline
    expect(@job.output(:out).read).to eq [1, 2]
    expect(@job.status).to eq 'completed'
    expect(@job.get(:wait_queue)).to be nil
  end

  it 'forwards multiple packets in order' do
    now = Time.now
    allow(Time).to receive(:now) { now }
    @job.input(:delay).write 10
    @job.input(:in).write 1
    @job.run_inline
    allow(Time).to receive(:now) { now + 5 }
    @job.input(:in).write 2
    @job.run_inline
    expect(@job.status).to eq 'queued' # should be scheduled
    expect(@job.output(:out).size).to eq 0
    allow(Time).to receive(:now) { now + 10 }
    @job.run_inline
    expect(@job.output(:out).size).to eq 1
    expect(@job.output(:out).read).to eq 1
    expect(@job.status).to eq 'queued'
    allow(Time).to receive(:now) { now + 15 }
    @job.run_inline
    expect(@job.output(:out).size).to eq 1
    expect(@job.output(:out).read).to eq 2
    expect(@job.status).to eq 'completed'
  end

  it 'correctly orders packets with different delays' do
    @job = SideJob.queue('core', 'Workers::Delay', inports: { delay: { mode: :queue }, in: {}})
    now = Time.now
    allow(Time).to receive(:now) { now }
    [10, 5].each {|x| @job.input(:delay).write x}
    ['a', 'b'].each {|x| @job.input(:in).write x}
    @job.run_inline
    expect(@job.status).to eq 'queued' # should be scheduled
    expect(@job.output(:out).size).to eq 0
    allow(Time).to receive(:now) { now + 10 }
    @job.run_inline
    expect(@job.output(:out).size).to eq 2
    expect(@job.output(:out).read).to eq 'b'
    expect(@job.output(:out).read).to eq 'a'
    expect(@job.status).to eq 'completed'
  end
end
