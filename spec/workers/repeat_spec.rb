require 'spec_helper'

describe Workers::Repeat do
  before do
    @job = SideJob.queue('core', 'Workers::Repeat')
  end

  it 'completes when no input' do
    @job.run_inline
    expect(@job.status).to eq 'completed'
  end

  it 'can repeat 0 times' do
    @job.input(:times).write(0)
    @job.input(:in).write('a')
    @job.run_inline
    expect(@job.output(:out).read).to eq []
    expect(@job.status).to eq 'completed'
  end

  it 'can repeat 1 time' do
    @job.input(:times).write(1)
    @job.input(:in).write(true)
    @job.run_inline
    expect(@job.output(:out).read).to eq [true]
    expect(@job.status).to eq 'completed'
  end

  it 'can repeat multiple times' do
    @job.input(:times).write(4)
    @job.input(:in).write(5)
    @job.run_inline
    expect(@job.output(:out).read).to eq [5, 5, 5, 5]
    expect(@job.status).to eq 'completed'
  end
end
