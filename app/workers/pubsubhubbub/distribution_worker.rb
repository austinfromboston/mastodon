# frozen_string_literal: true
module Pubsubhubbub
  class DistributionWorker
    include Sidekiq::Worker

    sidekiq_options queue: 'push'

    def self.perform_async_merged(stream_entry_id, merge_on:)
      merge_key = "enqueued-posts:#{merge_on}"
      Sidekiq.redis do |c|
        if c.lpush(merge_key, stream_entry_id) == 1
          perform_async stream_entry_id, merge_key
        end
      end
    end

    def perform(stream_entry_id, merge_key = nil)
      multiple_entries = enqueued_entries(merge_key)
      new_entries = multiple_entries || [stream_entry_id]
      entries_to_distribute = StreamEntry.where(id: new_entries, hidden: false)
      return unless entries_to_distribute.any?

      account = entries_to_distribute.first.account
      payload = AtomSerializer.render(AtomSerializer.new.feed(account, entries_to_distribute.to_a))

      Subscription.where(account: account).active.select('id, callback_url').find_each do |subscription|
        Pubsubhubbub::DeliveryWorker.perform_async(subscription.id, payload)
      end
    rescue StandardError => e
      # restore the redis state so Sidekiq retry mechanism will have the correct state
      restore_queued_entries(multiple_entries, merge_key)
      raise e
    end

    def restore_queued_entries(entries, merge_key)
      return unless entries.present? && merge_key.present?
      Sidekiq.redis do |c|
        c.multi do
          entries.each { |e| c.lpush(merge_key, e) }
        end
      end
    end

    def enqueued_entries(merge_key)
      return unless merge_key
      Sidekiq.redis do |c|
        entries = c.lrange(merge_key, 0, -1)
        entries.each { |e| c.lrem(merge_key, 0, e) }
      end
    end
  end
end
