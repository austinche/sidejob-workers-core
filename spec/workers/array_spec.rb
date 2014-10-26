require 'spec_helper'

describe Workers::Array do
  before do
    @job = SideJob.queue('core', 'Workers::Array', inports: {val1: {}, val2: {}})
  end

  it 'completes on no input' do
    @job.run_inline
    expect(@job.status).to eq 'completed'
  end

  it 'suspends when missing some inputs' do
    @job.input(:val1).write([1,2])
    @job.run_inline
    expect(@job.status).to eq 'suspended'
  end

  it 'saves and resumes with partially read inputs' do
    @job.input(:val1).write([1,2])
    @job.run_inline
    expect(@job.status).to eq 'suspended'
    @job.input(:val2).write true
    @job.run_inline
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).size).to eq 1
    expect(@job.output(:out).read).to eq [[1,2], true]
  end
end
