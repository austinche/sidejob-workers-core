require 'spec_helper'

describe Workers::Wait do
  before do
    @job = SideJob.queue('core', 'Workers::Wait')
  end

  it 'completes on no input' do
    @job.run_inline
    expect(@job.status).to eq 'completed'
  end

  it 'waits for trigger to send data' do
    @job.input(:in).write 123
    @job.run_inline
    expect(@job.output(:out).size).to eq 0
    expect(@job.status).to eq 'suspended'
  end

  it 'suspends until input' do
    @job.input(:trigger).write 123
    @job.run_inline
    expect(@job.status).to eq 'suspended'
  end

  it 'sends data on each trigger' do
    @job.input(:trigger).write nil
    @job.input(:trigger).write false
    @job.input(:in).write 123
    @job.input(:in).write 456
    @job.run_inline
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).read).to eq(123)
    expect(@job.output(:out).read).to eq(456)
  end
end
