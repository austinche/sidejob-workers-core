require 'spec_helper'

describe Workers::Concat do
  before do
    @job = SideJob.queue('core', 'Workers::Concat', inports: {in1: {}, in2: {}})
  end

  it 'completes on no input' do
    @job.run_inline
    expect(@job.status).to eq 'completed'
  end

  it 'suspends when missing some inputs' do
    @job.input(:in1).write [1,2]
    @job.run_inline
    expect(@job.status).to eq 'suspended'
  end

  it 'concats array in alphabetical order of port names' do
    [[1,2], [3,4]].each {|x| @job.input(:in1).write x}
    [[5,6], [7,8]].each {|x| @job.input(:in2).write x}
    @job.run_inline
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).size).to eq 2
    expect(@job.output(:out).read).to eq([1,2,5,6])
    expect(@job.output(:out).read).to eq([3,4,7,8])
  end

  it 'suspends if missing some inputs after some concatenating' do
    @job.input(:in1).write [1,2]
    [[5,6], [7,8]].each {|x| @job.input(:in2).write x}
    @job.run_inline
    expect(@job.status).to eq 'suspended'
    expect(@job.output(:out).size).to eq 1
    expect(@job.output(:out).read).to eq([1,2,5,6])
  end

  it 'resumes after suspend with partial inputs' do
    @job.input(:in1).write [1,2]
    @job.run_inline
    expect(@job.status).to eq 'suspended'
    @job.input(:in2).write [5,6]
    @job.run_inline
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).size).to eq 1
    expect(@job.output(:out).read).to eq([1,2,5,6])
  end
end
