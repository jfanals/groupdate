module Groupdate
  module Adapters
    class SQLiteAdapter < BaseAdapter
      def group_clause
        raise Groupdate::Error, "day_start not supported for SQLite" unless day_start.zero?

        query =
          if period == :week
            ["strftime('%Y-%m-%d 00:00:00 UTC', #{column}, '-6 days', ?)", "weekday #{(week_start + 1) % 7}"]
          elsif period == :custom
            ["datetime((strftime('%s', #{column}) / ?) * ?, 'unixepoch')", n_seconds, n_seconds]
          else
            format =
              case period
              when :minute_of_hour
                "%M"
              when :hour_of_day
                "%H"
              when :day_of_week
                "%w"
              when :day_of_month
                "%d"
              when :day_of_year
                "%j"
              when :month_of_year
                "%m"
              when :second
                "%Y-%m-%d %H:%M:%S UTC"
              when :minute
                "%Y-%m-%d %H:%M:00 UTC"
              when :hour
                "%Y-%m-%d %H:00:00 UTC"
              when :day
                "%Y-%m-%d 00:00:00 UTC"
              when :month
                "%Y-%m-01 00:00:00 UTC"
              when :quarter
                nil
              else # year
                "%Y-01-01 00:00:00 UTC"
              end

            ["strftime(?, #{column})", format]
          end

        # TODO week_start, day_start, custom period
        # TODO move to better place
        if !@time_zone.utc_offset.zero? || period == :quarter
          utc = ActiveSupport::TimeZone["UTC"]
          db = @relation.connection.raw_connection
          db.create_function("groupdate", 3) do |func, value, period, time_zone|
            if value.nil?
              func.result = nil
            else
              result = [value].group_by_period(period, time_zone: time_zone, dates: false) { utc.parse(value) }.keys[0]
              result = result.in_time_zone(utc).strftime("%Y-%m-%d %H:%M:%S") if result.is_a?(Time)
              func.result = result
            end
          end
          query = ["groupdate(#{column}, ?, ?)", period, @time_zone.tzinfo.name]
        end

        @relation.send(:sanitize_sql_array, query)
      end
    end
  end
end
