require 'spec_helper'

describe Workers::WaitEach do
  before do
    @job = SideJob.queue('core', 'Workers::WaitEach')
  end

  it 'completes on no input' do
    @job.run_inline
    expect(@job.status).to eq 'completed'
  end

  it 'waits for array elements' do
    array = [123, true, false, nil, "abc", {"x" => 3}, [1,2]]
    @job.input(:in).write array
    @job.run_inline
    expect(@job.status).to eq 'suspended'
    expect(@job.output(:out).size).to eq 0
    array.each {|x| @job.input(:each).write x}
    @job.run_inline
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).read).to eq array
  end

  it 'handles out of order array elements' do
    @job.input(:in).write [1,2,3,4]
    [2,4,3,1].each {|x| @job.input(:each).write x}
    @job.run_inline
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).read).to eq [1,2,3,4]
  end

  it 'handles duplicate array elements' do
    @job.input(:in).write [1,1,2,2]
    [1,2].each {|x| @job.input(:each).write x}
    @job.run_inline
    expect(@job.status).to eq 'suspended'
    expect(@job.output(:out).size).to eq 0
    [1,2].each {|x| @job.input(:each).write x}
    @job.run_inline
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).read).to eq [1,1,2,2]
  end

  it 'handles saving each values for the next input array' do
    @job.input(:in).write [1,2]
    [1,2,2,3].each {|x| @job.input(:each).write x}
    @job.run_inline
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).read).to eq [1,2]
    @job.input(:in).write [3,2]
    @job.run_inline
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).read).to eq [3,2]
  end
end
