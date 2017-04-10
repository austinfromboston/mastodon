# frozen_string_literal: true
require 'rails_helper'

RSpec.describe Pubsubhubbub::DistributionWorker do
  let(:author) { Fabricate(:account, username: 'tom') }
  let(:status) { Fabricate(:status, text: 'Hello @axel #test', account: author) }
  let(:stream_entry) { Fabricate :stream_entry, activity: status, account: author }
  let(:merge_key) { "enqueued-posts:#{author.id}" }

  before do
    stub_request(:post, "http://example.com/callback")
  end

  after do
    Redis.current.del(merge_key)
  end

  describe '.perform_async_merged' do

    subject { described_class.perform_async_merged stream_entry.id, merge_on: author.id }
    before do
      allow_any_instance_of(described_class).to receive(:perform)
    end

    it 'enqueues the job' do
      expect_any_instance_of(described_class)
        .to receive(:perform).with(stream_entry.id, merge_key).once
      subject
    end
    it 'should make an entry in redis' do
      expect { subject }.to change { Redis.current.exists(merge_key) }.to(true)
    end
    context 'when there is already an entry' do
      before do
        Redis.current.lpush(merge_key, 123)
      end

      it 'does not enqueue the job' do
        expect_any_instance_of(described_class)
          .to_not receive(:perform)
        subject
      end
      it 'appends to the entry' do
        expect { subject }.to change { Redis.current.llen(merge_key) }.by(1)
        expect(Redis.current.lpop(merge_key)).to eq(stream_entry.id.to_s)
      end
    end
  end

  describe '#perform' do
    let(:worker) { described_class.new }
    let!(:subscription) { Fabricate :subscription, account: author, confirmed: true, expires_at: 1.month.from_now }

    subject { worker.perform stream_entry.id }

    context 'when not passed a merge key' do
      it 'should deliver the single entry' do
        expect(Pubsubhubbub::DeliveryWorker).to receive(:perform_async) do |*args|
          expect(args.first).to eq(subscription.id)
          expect(args.second).to match(/<id>tag:[.\w]+,\d{4}-\d{2}-\d{2}:objectId=#{status.id}:objectType=Status<\/id>/)
        end
        subject
      end
      it 'should not check redis for additional entries' do
        expect(Redis.current).to_not receive(:lrange)
        subject
      end
    end
    context 'when passed a merge_key' do
      let(:status2) { Fabricate(:status, text: 'Thought you might like a toot #test', account: author) }
      let(:stream_entry2) { Fabricate :stream_entry, activity: status2, account: author }
      subject { worker.perform(stream_entry.id, merge_key) }
      before do
        Redis.current.lpush(merge_key, stream_entry.id)
        Redis.current.lpush(merge_key, stream_entry2.id)
      end
      it 'should check redis for additional entries and include them in the serialization' do
        expect(Pubsubhubbub::DeliveryWorker).to receive(:perform_async) do |*args|
          expect(args.first).to eq(subscription.id)
          expect(args.second).to match(/<id>tag:[.\w]+,\d{4}-\d{2}-\d{2}:objectId=#{status.id}:objectType=Status<\/id>/)
          expect(args.second).to match(/<id>tag:[.\w]+,\d{4}-\d{2}-\d{2}:objectId=#{status2.id}:objectType=Status<\/id>/)
        end
        subject
      end

      it 'clears redis list when complete' do
        subject
        expect(Redis.current.exists(merge_key)).to be_falsey
      end

      context 'when an error occurs' do
        it 'does not clear the merge key' do
          expect(Pubsubhubbub::DeliveryWorker).to receive(:perform_async).and_raise("a failure happened")
          expect { subject }.to raise_error("a failure happened")
          expect(Redis.current.lrange(merge_key, 0, -1)).to eq([stream_entry.to_param, stream_entry2.to_param])
        end
      end
    end
  end
end
