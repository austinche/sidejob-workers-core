require 'spec_helper'

describe Workers::Widget do
  before do
    @job = SideJob.queue('core', 'Workers::Widget')
  end

  it 'suspends' do
    @job.run_inline
    expect(@job.status).to eq 'suspended'
  end
end
