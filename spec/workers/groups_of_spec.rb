require 'spec_helper'

describe Workers::GroupsOf do
  before do
    @job = SideJob.queue('core', 'Workers::GroupsOf')
  end

  it 'completes when no input' do
    @job.run_inline
    expect(@job.status).to eq 'completed'
  end

  it 'raises error if size is 0' do
    @job.input(:in).write (1..10).to_a
    @job.input(:size).write(0)
    expect { @job.run_inline }.to raise_error
  end

  it 'splits without fill' do
    @job.input(:in).write (1..10).to_a
    @job.input(:size).write(3)
    @job.run_inline
    expect(@job.output(:out).read).to eq [[1,2,3],[4,5,6],[7,8,9],[10]]
  end

  it 'can add filler' do
    @job.input(:in).write (1..10).to_a
    @job.input(:size).write(3)
    @job.input(:fill).write(nil)
    @job.run_inline
    expect(@job.output(:out).read).to eq [[1,2,3],[4,5,6],[7,8,9],[10,nil,nil]]
  end

  it 'does not add any filler if exactly multiple of size' do
    @job.input(:in).write (1..10).to_a
    @job.input(:size).write(5)
    @job.input(:fill).write(nil)
    @job.run_inline
    expect(@job.output(:out).read).to eq [[1,2,3,4,5],[6,7,8,9,10]]
  end
end
