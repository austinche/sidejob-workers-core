require 'spec_helper'

describe Workers::Map do
  before do
    @job = SideJob.queue('core', 'Workers::Map')
  end

  it 'completes on no input' do
    @job.run_inline
    expect(@job.status).to eq 'completed'
  end

  it 'takes input array and outputs elements' do
    @job.input(:in).write((0..4).to_a)
    @job.run_inline
    expect(@job.status).to eq 'suspended'
    expect(@job.output(:each).entries).to eq (0..4).to_a
  end

  it 'raises error if input is not an array' do
    @job.input(:in).write(3)
    expect { @job.run_inline }.to raise_error
  end

  it 'suspends on partial mapped values' do
    @job.input(:in).write((0..4).to_a)
    @job.input(:each).write('a')
    @job.run_inline
    expect(@job.status).to eq 'suspended'
  end

  it 'completes on all mapped values' do
    @job.input(:in).write((1..3).to_a)
    @job.input(:each).write('a')
    @job.run_inline
    @job.input(:each).write('b')
    @job.input(:each).write('c')
    @job.run_inline
    expect(@job.output(:each).entries).to eq (1..3).to_a
    expect(@job.output(:out).read).to eq ['a', 'b', 'c']
    expect(@job.status).to eq 'completed'
  end

  it 'can map with a default value port' do
    @job.input(:in).write((1..3).to_a)
    @job.input(:each).default = 'xyz'
    @job.run_inline
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).read).to eq ['xyz', 'xyz', 'xyz']
  end

  it 'can map on an object' do
    @job.input(:in).write({abc: 123, xyz: 456})
    @job.run_inline
    expect(@job.output(:each).entries).to match_array([['abc', 123], ['xyz', 456]])
    @job.input(:each).write('a')
    @job.input(:each).write('b')
    @job.input(:each).write('c')
    @job.run_inline
    expect(@job.output(:out).read).to eq ['a', 'b']
    expect(@job.status).to eq 'completed'
  end

end
