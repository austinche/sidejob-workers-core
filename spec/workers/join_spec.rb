require 'spec_helper'

describe Workers::Join do
  before do
    @job = SideJob.queue('core', 'Workers::Join')
    @data = ['hello', 'world', 1, 2, 3]
  end

  it 'joins with , by default' do
    @job.input(:in).write @data
    @job.run_inline
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).read).to eq @data.join(',')
  end

  it 'can use alternate separator' do
    @job.input(:sep).write "\t"
    @job.input(:in).write @data
    @job.run_inline
    expect(@job.status).to eq 'completed'
    expect(@job.output(:out).read).to eq @data.join("\t")
  end
end
