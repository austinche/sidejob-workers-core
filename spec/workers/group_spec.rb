require 'spec_helper'

describe Workers::Group do
  before do
    @job = SideJob.queue('core', 'Workers::Group')
  end

  it 'raises error if in is memory port' do
    @job = SideJob.queue('core', 'Workers::Group', inports: {in: {mode: :memory}})
    expect { @job.run_inline }.to raise_error
  end

  it 'raises error if n is less than 0' do
    @job.input(:n).write -1
    expect { @job.run_inline }.to raise_error
  end

  it 'suspends on no input' do
    @job.run_inline
    expect(@job.status).to eq 'suspended'
  end

  it 'suspends until n data have arrived' do
    @job.input(:n).write 3
    [1,2].each {|x| @job.input(:in).write x}
    @job.run_inline
    expect(@job.status).to eq 'suspended'
  end

  it 'sends out array of n data' do
    @job.input(:n).write 3
    ['a', 'b', 'c'].each {|x| @job.input(:in).write x}
    @job.run_inline
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).read).to eq ['a', 'b', 'c']
  end

  it 'handles a multiple of n data' do
    @job.input(:n).write 3
    ['a', 'b', 'c', 1, 2, 3].each {|x| @job.input(:in).write x}
    @job.run_inline
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).read).to eq ['a', 'b', 'c']
    expect(@job.output(:out).read).to eq [1, 2, 3]
  end

  it 'handles a non-multiple of n data' do
    @job.input(:n).write 3
    ['a', 'b', 'c', 'd'].each {|x| @job.input(:in).write x}
    @job.run_inline
    expect(@job.status).to eq 'suspended'
    expect(@job.output(:out).read).to eq ['a', 'b', 'c']
    expect(@job.output(:out).data?).to be false
  end

  it 'handles data over time' do
    @job.input(:n).write 3
    ['a', 'b'].each {|x| @job.input(:in).write x}
    @job.run_inline
    expect(@job.status).to eq 'suspended'
    @job.input(:in).write 'c'
    @job.run_inline
    expect(@job.output(:out).read).to eq ['a', 'b', 'c']
  end

  it 'properly persists unwritten stored data' do
    @job.input(:n).write 3
    @job.input(:in).write 'a'
    @job.run_inline
    ['b', 'c'].each {|x| @job.input(:in).write x}
    @job.run_inline
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).read).to eq ['a', 'b', 'c']
    ['d', 'e', 'f'].each {|x| @job.input(:in).write x}
    @job.run_inline
    expect(@job.output(:out).read).to eq ['d', 'e', 'f']
  end
end
