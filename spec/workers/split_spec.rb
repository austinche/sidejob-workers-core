require 'spec_helper'

describe Workers::Split do
  before do
    @job = SideJob.queue('core', 'Workers::Split')
  end

  it 'splits with , by default' do
    @job.input(:in).write '1,2,3,4'
    @job.run_inline
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).read).to eq ['1','2','3','4']
  end

  it 'can use alternate separator' do
    @job.input(:sep).write '-'
    @job.input(:in).write '1-2-3-4'
    @job.run_inline
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).read).to eq ['1','2','3','4']
  end
end
