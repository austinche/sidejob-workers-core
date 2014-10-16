require 'spec_helper'

describe Workers::Flatten do
  before do
    @job = SideJob.queue('core', 'Workers::Flatten')
  end

  it 'fully flattens an array' do
    @job.input(:in).write([1,2,[3,4,[5,6]]])
    @job.run_inline
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).read).to eq [1,2,3,4,5,6]
  end

  it 'can specify flatten level' do
    @job.input(:level).write 1
    @job.input(:in).write([1,2,[3,4,[5,6]]])
    @job.run_inline
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).read).to eq [1,2,3,4,[5,6]]
  end
end
