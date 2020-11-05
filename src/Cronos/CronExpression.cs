// The MIT License(MIT)
// 
// Copyright (c) 2017 Sergey Odinokov
// 
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
// 
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
// 
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.

using System;
using System.Collections.Generic;
using System.Diagnostics.CodeAnalysis;
using System.Runtime.CompilerServices;
using System.Text;

namespace Cronos
{
    /// <summary>
    /// Provides a parser and scheduler for cron expressions.
    /// </summary>
    public sealed class CronExpression: IEquatable<CronExpression>
    {
        private const long NotFound = 0;

        private const int MinNthDayOfWeek = 1;
        private const int MaxNthDayOfWeek = 5;
        private const int SundayBits = 0b1000_0001;

        private const int MaxYear = 2099;

        private static readonly TimeZoneInfo UtcTimeZone = TimeZoneInfo.Utc;

        private static readonly CronExpression Yearly = Parse("0 0 1 1 *");
        private static readonly CronExpression Weekly = Parse("0 0 * * 0");
        private static readonly CronExpression Monthly = Parse("0 0 1 * *");
        private static readonly CronExpression Daily = Parse("0 0 * * *");
        private static readonly CronExpression Hourly = Parse("0 * * * *");
        private static readonly CronExpression Minutely = Parse("* * * * *");
        private static readonly CronExpression Secondly = Parse("* * * * * *", CronFormat.IncludeSeconds);

        private static readonly int[] DeBruijnPositions =
        {
            0, 1, 2, 53, 3, 7, 54, 27,
            4, 38, 41, 8, 34, 55, 48, 28,
            62, 5, 39, 46, 44, 42, 22, 9,
            24, 35, 59, 56, 49, 18, 29, 11,
            63, 52, 6, 26, 37, 40, 33, 47,
            61, 45, 43, 21, 23, 58, 17, 10,
            51, 25, 36, 32, 60, 20, 57, 16,
            50, 31, 19, 15, 30, 14, 13, 12
        };

        private long  _second;     // 60 bits -> from 0 bit to 59 bit
        private long  _minute;     // 60 bits -> from 0 bit to 59 bit
        private int   _hour;       // 24 bits -> from 0 bit to 23 bit
        private int   _dayOfMonth; // 31 bits -> from 1 bit to 31 bit
        private short _month;      // 12 bits -> from 1 bit to 12 bit
        private byte  _dayOfWeek;  // 8 bits  -> from 0 bit to 7 bit

        private byte  _nthDayOfWeek;
        private byte  _lastMonthOffset;

        private CronExpressionFlag _flags;

        private CronExpression()
        {
        }

        ///<summary>
        /// Constructs a new <see cref="CronExpression"/> based on the specified
        /// cron expression. It's supported expressions consisting of 5 fields:
        /// minute, hour, day of month, month, day of week. 
        /// If you want to parse non-standard cron expressions use <see cref="Parse(string, CronFormat)"/> with specified CronFields argument.
        /// See more: <a href="https://github.com/HangfireIO/Cronos">https://github.com/HangfireIO/Cronos</a>
        /// </summary>
        public static CronExpression Parse(string expression)
        {
            return Parse(expression, CronFormat.Standard);
        }

        ///<summary>
        /// Constructs a new <see cref="CronExpression"/> based on the specified
        /// cron expression. It's supported expressions consisting of 5 or 6 fields:
        /// second (optional), minute, hour, day of month, month, day of week. 
        /// See more: <a href="https://github.com/HangfireIO/Cronos">https://github.com/HangfireIO/Cronos</a>
        /// </summary>
#if !NET40
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        public static CronExpression Parse(string expression, CronFormat format)
        {
            if (string.IsNullOrEmpty(expression)) throw new ArgumentNullException(nameof(expression));

            var pointer = 0;

            SkipWhiteSpaces(ref pointer, expression);

            CronExpression cronExpression;

            if (Accept(ref pointer, expression, '@'))
            {
                cronExpression = ParseMacro(ref pointer, expression);
                SkipWhiteSpaces(ref pointer, expression);

                if (cronExpression == null || !IsEndOfString(pointer, expression)) ThrowFormatException("Macro: Unexpected character '{0}' on position {1}.", GetChar(pointer, expression), pointer);

                return cronExpression;
            }

            cronExpression = new CronExpression();

            if (format == CronFormat.IncludeSeconds)
            {
                cronExpression._second = ParseField(CronField.Seconds, ref pointer, expression, ref cronExpression._flags);
                ParseWhiteSpace(CronField.Seconds, ref pointer, expression);
            }
            else
            {
                SetBit(ref cronExpression._second, CronField.Seconds.First);
            }

            cronExpression._minute = ParseField(CronField.Minutes, ref pointer, expression, ref cronExpression._flags);
            ParseWhiteSpace(CronField.Minutes, ref pointer, expression);

            cronExpression._hour = (int)ParseField(CronField.Hours, ref pointer, expression, ref cronExpression._flags);
            ParseWhiteSpace(CronField.Hours, ref pointer, expression);

            cronExpression._dayOfMonth = (int)ParseDayOfMonth(ref pointer, expression, ref cronExpression._flags, ref cronExpression._lastMonthOffset);
            ParseWhiteSpace(CronField.DaysOfMonth, ref pointer, expression);

            cronExpression._month = (short)ParseField(CronField.Months, ref pointer, expression, ref cronExpression._flags);
            ParseWhiteSpace(CronField.Months, ref pointer, expression);

            cronExpression._dayOfWeek = (byte)ParseDayOfWeek(ref pointer, expression, ref cronExpression._flags, ref cronExpression._nthDayOfWeek);
            ParseEndOfString(ref pointer, expression);

            // Make sundays equivalent.
            if ((cronExpression._dayOfWeek & SundayBits) != 0)
            {
                cronExpression._dayOfWeek |= SundayBits;
            }

            return cronExpression;

        }

        /// <summary>
        /// Calculates next occurrence starting with <paramref name="fromUtc"/> (optionally <paramref name="inclusive"/>) in UTC time zone.
        /// </summary>
        public DateTime? GetNextOccurrence(DateTime fromUtc, bool inclusive = false)
        {
            if (fromUtc.Kind != DateTimeKind.Utc) ThrowWrongDateTimeKindException(nameof(fromUtc));

            var found = FindOccurence(fromUtc.Ticks, inclusive);
            if (found == NotFound) return null;

            return new DateTime(found, DateTimeKind.Utc);
        }

        /// <summary>
        /// Returns the list of next occurrences within the given date/time range,
        /// including <paramref name="fromUtc"/> and excluding <paramref name="toUtc"/>
        /// by default, and UTC time zone. When none of the occurrences found, an 
        /// empty list is returned.
        /// </summary>
        public IEnumerable<DateTime> GetOccurrences(
            DateTime fromUtc,
            DateTime toUtc,
            bool fromInclusive = true,
            bool toInclusive = false)
        {
            if (fromUtc > toUtc) ThrowFromShouldBeLessThanToException(nameof(fromUtc), nameof(toUtc));

            for (var occurrence = GetNextOccurrence(fromUtc, fromInclusive);
                occurrence < toUtc || occurrence == toUtc && toInclusive;
                // ReSharper disable once RedundantArgumentDefaultValue
                // ReSharper disable once ArgumentsStyleLiteral
                occurrence = GetNextOccurrence(occurrence.Value, inclusive: false))
            {
                yield return occurrence.Value;
            }
        }

        /// <summary>
        /// Calculates next occurrence starting with <paramref name="fromUtc"/> (optionally <paramref name="inclusive"/>) in given <paramref name="zone"/>
        /// </summary>
        public DateTime? GetNextOccurrence(DateTime fromUtc, TimeZoneInfo zone, bool inclusive = false)
        {
            if (fromUtc.Kind != DateTimeKind.Utc) ThrowWrongDateTimeKindException(nameof(fromUtc));

            if (ReferenceEquals(zone, UtcTimeZone))
            {
                var found = FindOccurence(fromUtc.Ticks, inclusive);
                if (found == NotFound) return null;

                return new DateTime(found, DateTimeKind.Utc);
            }

            var zonedStart = TimeZoneInfo.ConvertTime(fromUtc, zone);
            var zonedStartOffset = new DateTimeOffset(zonedStart, zonedStart - fromUtc);
            var occurrence = GetOccurenceByZonedTimes(zonedStartOffset, zone, inclusive);
            return occurrence?.UtcDateTime;
        }

        /// <summary>
        /// Returns the list of next occurrences within the given date/time range, including
        /// <paramref name="fromUtc"/> and excluding <paramref name="toUtc"/> by default, and 
        /// specified time zone. When none of the occurrences found, an empty list is returned.
        /// </summary>
        public IEnumerable<DateTime> GetOccurrences(
            DateTime fromUtc,
            DateTime toUtc,
            TimeZoneInfo zone,
            bool fromInclusive = true,
            bool toInclusive = false)
        {
            if (fromUtc > toUtc) ThrowFromShouldBeLessThanToException(nameof(fromUtc), nameof(toUtc));

            for (var occurrence = GetNextOccurrence(fromUtc, zone, fromInclusive);
                occurrence < toUtc || occurrence == toUtc && toInclusive;
                // ReSharper disable once RedundantArgumentDefaultValue
                // ReSharper disable once ArgumentsStyleLiteral
                occurrence = GetNextOccurrence(occurrence.Value, zone, inclusive: false))
            {
                yield return occurrence.Value;
            }
        }

        /// <summary>
        /// Calculates next occurrence starting with <paramref name="from"/> (optionally <paramref name="inclusive"/>) in given <paramref name="zone"/>
        /// </summary>
        public DateTimeOffset? GetNextOccurrence(DateTimeOffset from, TimeZoneInfo zone, bool inclusive = false)
        {
            if (ReferenceEquals(zone, UtcTimeZone))
            {
                var found = FindOccurence(from.UtcTicks, inclusive);
                if (found == NotFound) return null;

                return new DateTimeOffset(found, TimeSpan.Zero);
            }

            var zonedStart = TimeZoneInfo.ConvertTime(from, zone);
            return GetOccurenceByZonedTimes(zonedStart, zone, inclusive);
        }

        /// <summary>
        /// Returns the list of occurrences within the given date/time offset range,
        /// including <paramref name="from"/> and excluding <paramref name="to"/> by
        /// default. When none of the occurrences found, an empty list is returned.
        /// </summary>
        public IEnumerable<DateTimeOffset> GetOccurrences(
            DateTimeOffset from,
            DateTimeOffset to,
            TimeZoneInfo zone,
            bool fromInclusive = true,
            bool toInclusive = false)
        {
            if (from > to) ThrowFromShouldBeLessThanToException(nameof(from), nameof(to));

            for (var occurrence = GetNextOccurrence(from, zone, fromInclusive);
                occurrence < to || occurrence == to && toInclusive;
                // ReSharper disable once RedundantArgumentDefaultValue
                // ReSharper disable once ArgumentsStyleLiteral
                occurrence = GetNextOccurrence(occurrence.Value, zone, inclusive: false))
            {
                yield return occurrence.Value;
            }
        }

        /// <inheritdoc />
        public override string ToString()
        {
            var expressionBuilder = new StringBuilder();

            AppendFieldValue(expressionBuilder, CronField.Seconds, _second).Append(' ');
            AppendFieldValue(expressionBuilder, CronField.Minutes, _minute).Append(' ');
            AppendFieldValue(expressionBuilder, CronField.Hours, _hour).Append(' ');
            AppendDayOfMonth(expressionBuilder, _dayOfMonth).Append(' ');
            AppendFieldValue(expressionBuilder, CronField.Months, _month).Append(' ');
            AppendDayOfWeek(expressionBuilder, _dayOfWeek);

            return expressionBuilder.ToString();
        }

        /// <summary>
        /// Determines whether the specified <see cref="Object"/> is equal to the current <see cref="Object"/>.
        /// </summary>
        /// <param name="other">The <see cref="Object"/> to compare with the current <see cref="Object"/>.</param>
        /// <returns>
        /// <c>true</c> if the specified <see cref="Object"/> is equal to the current <see cref="Object"/>; otherwise, <c>false</c>.
        /// </returns>
        public bool Equals(CronExpression other)
        {
            if (other == null) return false;

            return _second == other._second &&
                   _minute == other._minute &&
                   _hour == other._hour &&
                   _dayOfMonth == other._dayOfMonth &&
                   _month == other._month &&
                   _dayOfWeek == other._dayOfWeek &&
                   _nthDayOfWeek == other._nthDayOfWeek &&
                   _lastMonthOffset == other._lastMonthOffset &&
                   _flags == other._flags;
        }

        /// <summary>
        /// Determines whether the specified <see cref="System.Object" /> is equal to this instance.
        /// </summary>
        /// <param name="obj">The <see cref="System.Object" /> to compare with this instance.</param>
        /// <returns>
        /// <c>true</c> if the specified <see cref="System.Object" /> is equal to this instance;
        /// otherwise, <c>false</c>.
        /// </returns>
        public override bool Equals(object obj) => Equals(obj as CronExpression);

        /// <summary>
        /// Returns a hash code for this instance.
        /// </summary>
        /// <returns>
        /// A hash code for this instance, suitable for use in hashing algorithms and data
        /// structures like a hash table. 
        /// </returns>
        [SuppressMessage("ReSharper", "NonReadonlyMemberInGetHashCode")]
        public override int GetHashCode()
        {
            unchecked
            {
                var hashCode = _second.GetHashCode();
                hashCode = (hashCode * 397) ^ _minute.GetHashCode();
                hashCode = (hashCode * 397) ^ _hour;
                hashCode = (hashCode * 397) ^ _dayOfMonth;
                hashCode = (hashCode * 397) ^ _month.GetHashCode();
                hashCode = (hashCode * 397) ^ _dayOfWeek.GetHashCode();
                hashCode = (hashCode * 397) ^ _nthDayOfWeek.GetHashCode();
                hashCode = (hashCode * 397) ^ _lastMonthOffset.GetHashCode();
                hashCode = (hashCode * 397) ^ (int)_flags;

                return hashCode;
            }
        }

        /// <summary>
        /// Implements the operator ==.
        /// </summary>
        public static bool operator ==(CronExpression left, CronExpression right) => Equals(left, right);

        /// <summary>
        /// Implements the operator !=.
        /// </summary>
        public static bool operator !=(CronExpression left, CronExpression right) => !Equals(left, right);


        private DateTimeOffset? GetOccurenceByZonedTimes(DateTimeOffset from, TimeZoneInfo zone, bool inclusive)
        {
            var fromLocal = from.DateTime;

            if (TimeZoneHelper.IsAmbiguousTime(zone, fromLocal))
            {
                var currentOffset = from.Offset;
                var standardOffset = zone.BaseUtcOffset;

                if (standardOffset != currentOffset)
                {
                    var daylightOffset = TimeZoneHelper.GetDaylightOffset(zone, fromLocal);
                    var daylightTimeLocalEnd = TimeZoneHelper.GetDaylightTimeEnd(zone, fromLocal, daylightOffset).DateTime;

                    // Early period, try to find anything here.
                    var foundInDaylightOffset = FindOccurence(fromLocal.Ticks, daylightTimeLocalEnd.Ticks, inclusive);
                    if (foundInDaylightOffset != NotFound) return new DateTimeOffset(foundInDaylightOffset, daylightOffset);

                    fromLocal = TimeZoneHelper.GetStandardTimeStart(zone, fromLocal, daylightOffset).DateTime;
                    inclusive = true;
                }

                // Skip late ambiguous interval.
                var ambiguousIntervalLocalEnd = TimeZoneHelper.GetAmbiguousIntervalEnd(zone, fromLocal).DateTime;

                if (HasFlag(CronExpressionFlag.Interval))
                {
                    var foundInStandardOffset = FindOccurence(fromLocal.Ticks, ambiguousIntervalLocalEnd.Ticks - 1, inclusive);
                    if (foundInStandardOffset != NotFound) return new DateTimeOffset(foundInStandardOffset, standardOffset);
                }

                fromLocal = ambiguousIntervalLocalEnd;
                inclusive = true;
            }

            var occurrenceTicks = FindOccurence(fromLocal.Ticks, inclusive);
            if (occurrenceTicks == NotFound) return null;

            var occurrence = new DateTime(occurrenceTicks);

            if (zone.IsInvalidTime(occurrence))
            {
                var nextValidTime = TimeZoneHelper.GetDaylightTimeStart(zone, occurrence);
                return nextValidTime;
            }

            if (TimeZoneHelper.IsAmbiguousTime(zone, occurrence))
            {
                var daylightOffset = TimeZoneHelper.GetDaylightOffset(zone, occurrence);
                return new DateTimeOffset(occurrence, daylightOffset);
            }

            return new DateTimeOffset(occurrence, zone.GetUtcOffset(occurrence));
        }

        private long FindOccurence(long startTimeTicks, long endTimeTicks, bool startInclusive)
        {
            var found = FindOccurence(startTimeTicks, startInclusive);

            if (found == NotFound || found > endTimeTicks) return NotFound;
            return found;
        }

#if !NET40
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private static char GetChar(int pointer, string expression)
        {
            return pointer >= expression.Length ? '\0' : expression[pointer];
        }

        private long FindOccurence(long ticks, bool startInclusive)
        {
            if (!startInclusive) ticks++;

            CalendarHelper.FillDateTimeParts(
                ticks,
                out int startSecond,
                out int startMinute,
                out int startHour,
                out int startDay,
                out int startMonth,
                out int startYear);

            var minMatchedDay = GetFirstSet(_dayOfMonth);

            var second = startSecond;
            var minute = startMinute;
            var hour = startHour;
            var day = startDay;
            var month = startMonth;
            var year = startYear;

            if (!GetBit(_second, second) && !Move(_second, ref second)) minute++;
            if (!GetBit(_minute, minute) && !Move(_minute, ref minute)) hour++;
            if (!GetBit(_hour, hour) && !Move(_hour, ref hour)) day++;

            // If NearestWeekday flag is set it's possible forward shift.
            if (HasFlag(CronExpressionFlag.NearestWeekday)) day = CronField.DaysOfMonth.First;

            if (!GetBit(_dayOfMonth, day) && !Move(_dayOfMonth, ref day)) goto RetryMonth;
            if (!GetBit(_month, month)) goto RetryMonth;

            Retry:

            if (day > GetLastDayOfMonth(year, month)) goto RetryMonth;

            if (HasFlag(CronExpressionFlag.DayOfMonthLast)) day = GetLastDayOfMonth(year, month);

            var lastCheckedDay = day;

            if (HasFlag(CronExpressionFlag.NearestWeekday)) day = CalendarHelper.MoveToNearestWeekDay(year, month, day);

            if (IsDayOfWeekMatch(year, month, day))
            {
                if (CalendarHelper.IsGreaterThan(year, month, day, startYear, startMonth, startDay)) goto RolloverDay;
                if (hour > startHour) goto RolloverHour;
                if (minute > startMinute) goto RolloverMinute;
                goto ReturnResult;

                RolloverDay: hour = GetFirstSet(_hour);
                RolloverHour: minute = GetFirstSet(_minute);
                RolloverMinute: second = GetFirstSet(_second);

                ReturnResult:

                var found = CalendarHelper.DateTimeToTicks(year, month, day, hour, minute, second);
                if (found >= ticks) return found;
            }

            day = lastCheckedDay;
            if (Move(_dayOfMonth, ref day)) goto Retry;

            RetryMonth:

            if (!Move(_month, ref month) && ++year >= MaxYear) return NotFound;
            day = minMatchedDay;

            goto Retry;
        }

        private static bool Move(long fieldBits, ref int fieldValue)
        {
            if (fieldBits >> ++fieldValue == 0)
            {
                fieldValue = GetFirstSet(fieldBits);
                return false;
            }

            fieldValue += GetFirstSet(fieldBits >> fieldValue);
            return true;
        }

        private int GetLastDayOfMonth(int year, int month)
        {
            return CalendarHelper.GetDaysInMonth(year, month) - _lastMonthOffset;
        }

        private bool IsDayOfWeekMatch(int year, int month, int day)
        {
            if (HasFlag(CronExpressionFlag.DayOfWeekLast) && !CalendarHelper.IsLastDayOfWeek(year, month, day) ||
                HasFlag(CronExpressionFlag.NthDayOfWeek) && !CalendarHelper.IsNthDayOfWeek(day, _nthDayOfWeek))
            {
                return false;
            }

            if (_dayOfWeek == CronField.DaysOfWeek.AllBits) return true;

            var dayOfWeek = CalendarHelper.GetDayOfWeek(year, month, day);

            return ((_dayOfWeek >> (int)dayOfWeek) & 1) != 0;
        }

#if !NET40
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private static int GetFirstSet(long value)
        {
            // TODO: Add description and source
            ulong res = unchecked((ulong)(value & -value) * 0x022fdd63cc95386d) >> 58;
            return DeBruijnPositions[res];
        }

        private bool HasFlag(CronExpressionFlag value)
        {
            return (_flags & value) != 0;
        }

#if !NET40
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private static void SkipWhiteSpaces(ref int pointer, string expression)
        {
            while (IsWhiteSpace(pointer, expression)) { pointer++; }
        }

#if !NET40
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private static void ParseWhiteSpace(CronField prevField, ref int pointer, string expression)
        {
            if (!IsWhiteSpace(pointer, expression)) ThrowFormatException(prevField, "Unexpected character '{0}'.", GetChar(pointer, expression));
            SkipWhiteSpaces(ref pointer, expression);
        }

#if !NET40
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private static void ParseEndOfString(ref int pointer, string expression)
        {
            if (!IsEndOfString(pointer, expression) && !IsWhiteSpace(pointer, expression)) ThrowFormatException(CronField.DaysOfWeek, "Unexpected character '{0}'.", GetChar(pointer, expression));

            SkipWhiteSpaces(ref pointer, expression);
            if (!IsEndOfString(pointer, expression)) ThrowFormatException("Unexpected character '{0}'.", GetChar(pointer, expression));
        }

        private static CronExpression ParseMacro(ref int pointer, string expression)
        {
            switch (ToUpper(GetChar(pointer++, expression)))
            {
                case 'A':
                    if (AcceptCharacter(ref pointer, expression, 'N') &&
                        AcceptCharacter(ref pointer, expression, 'N') &&
                        AcceptCharacter(ref pointer, expression, 'U') &&
                        AcceptCharacter(ref pointer, expression, 'A') &&
                        AcceptCharacter(ref pointer, expression, 'L') &&
                        AcceptCharacter(ref pointer, expression, 'L') &&
                        AcceptCharacter(ref pointer, expression, 'Y'))
                        return Yearly;
                    return null;
                case 'D':
                    if (AcceptCharacter(ref pointer, expression, 'A') &&
                        AcceptCharacter(ref pointer, expression, 'I') &&
                        AcceptCharacter(ref pointer, expression, 'L') &&
                        AcceptCharacter(ref pointer, expression, 'Y'))
                        return Daily;
                    return null;
                case 'E':
                    if (AcceptCharacter(ref pointer, expression, 'V') &&
                        AcceptCharacter(ref pointer, expression, 'E') &&
                        AcceptCharacter(ref pointer, expression, 'R') &&
                        AcceptCharacter(ref pointer, expression, 'Y') &&
                        Accept(ref pointer, expression, '_'))
                    {
                        if (AcceptCharacter(ref pointer, expression, 'M') &&
                            AcceptCharacter(ref pointer, expression, 'I') &&
                            AcceptCharacter(ref pointer, expression, 'N') &&
                            AcceptCharacter(ref pointer, expression, 'U') &&
                            AcceptCharacter(ref pointer, expression, 'T') &&
                            AcceptCharacter(ref pointer, expression, 'E'))
                            return Minutely;

                        if (GetChar(pointer - 1, expression) != '_') return null;

                        if (AcceptCharacter(ref pointer, expression, 'S') &&
                            AcceptCharacter(ref pointer, expression, 'E') &&
                            AcceptCharacter(ref pointer, expression, 'C') &&
                            AcceptCharacter(ref pointer, expression, 'O') &&
                            AcceptCharacter(ref pointer, expression, 'N') &&
                            AcceptCharacter(ref pointer, expression, 'D'))
                            return Secondly;
                    }

                    return null;
                case 'H':
                    if (AcceptCharacter(ref pointer, expression, 'O') &&
                        AcceptCharacter(ref pointer, expression, 'U') &&
                        AcceptCharacter(ref pointer, expression, 'R') &&
                        AcceptCharacter(ref pointer, expression, 'L') &&
                        AcceptCharacter(ref pointer, expression, 'Y'))
                        return Hourly;
                    return null;
                case 'M':
                    if (AcceptCharacter(ref pointer, expression, 'O') &&
                        AcceptCharacter(ref pointer, expression, 'N') &&
                        AcceptCharacter(ref pointer, expression, 'T') &&
                        AcceptCharacter(ref pointer, expression, 'H') &&
                        AcceptCharacter(ref pointer, expression, 'L') &&
                        AcceptCharacter(ref pointer, expression, 'Y'))
                        return Monthly;

                    if (ToUpper(GetChar(pointer - 1, expression)) == 'M' &&
                        AcceptCharacter(ref pointer, expression, 'I') &&
                        AcceptCharacter(ref pointer, expression, 'D') &&
                        AcceptCharacter(ref pointer, expression, 'N') &&
                        AcceptCharacter(ref pointer, expression, 'I') &&
                        AcceptCharacter(ref pointer, expression, 'G') &&
                        AcceptCharacter(ref pointer, expression, 'H') &&
                        AcceptCharacter(ref pointer, expression, 'T'))
                        return Daily;

                    return null;
                case 'W':
                    if (AcceptCharacter(ref pointer, expression, 'E') &&
                        AcceptCharacter(ref pointer, expression, 'E') &&
                        AcceptCharacter(ref pointer, expression, 'K') &&
                        AcceptCharacter(ref pointer, expression, 'L') &&
                        AcceptCharacter(ref pointer, expression, 'Y'))
                        return Weekly;
                    return null;
                case 'Y':
                    if (AcceptCharacter(ref pointer, expression, 'E') &&
                        AcceptCharacter(ref pointer, expression, 'A') &&
                        AcceptCharacter(ref pointer, expression, 'R') &&
                        AcceptCharacter(ref pointer, expression, 'L') &&
                        AcceptCharacter(ref pointer, expression, 'Y'))
                        return Yearly;
                    return null;
                default:
                    pointer--;
                    return null;
            }
        }

        private static long ParseField(CronField field, ref int pointer, string expression, ref CronExpressionFlag flags)
        {
            if (Accept(ref pointer, expression, '*') || Accept(ref pointer, expression, '?'))
            {
                if (field.CanDefineInterval) flags |= CronExpressionFlag.Interval;
                return ParseStar(field, ref pointer, expression);
            }

            var num = ParseValue(field, ref pointer, expression);

            var bits = ParseRange(field, ref pointer, expression, num, ref flags);
            if (Accept(ref pointer, expression, ',')) bits |= ParseList(field, ref pointer, expression, ref flags);

            return bits;
        }

        private static long ParseDayOfMonth(ref int pointer, string expression, ref CronExpressionFlag flags, ref byte lastDayOffset)
        {
            var field = CronField.DaysOfMonth;

            if (Accept(ref pointer, expression, '*') || Accept(ref pointer, expression, '?')) return ParseStar(field, ref pointer, expression);

            if (AcceptCharacter(ref pointer, expression, 'L')) return ParseLastDayOfMonth(field, ref pointer, expression, ref flags, ref lastDayOffset);

            var dayOfMonth = ParseValue(field, ref pointer, expression);

            if (AcceptCharacter(ref pointer, expression, 'W'))
            {
                flags |= CronExpressionFlag.NearestWeekday;
                return GetBit(dayOfMonth);
            }

            var bits = ParseRange(field, ref pointer, expression, dayOfMonth, ref flags);
            if (Accept(ref pointer, expression, ',')) bits |= ParseList(field, ref pointer, expression, ref flags);

            return bits;
        }

#if !NET40
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private static long ParseDayOfWeek(ref int pointer, string expression, ref CronExpressionFlag flags, ref byte nthWeekDay)
        {
            var field = CronField.DaysOfWeek;
            if (Accept(ref pointer, expression, '*') || Accept(ref pointer, expression, '?')) return ParseStar(field, ref pointer, expression);

            var dayOfWeek = ParseValue(field, ref pointer, expression);

            if (AcceptCharacter(ref pointer, expression, 'L')) return ParseLastWeekDay(dayOfWeek, ref flags);
            if (Accept(ref pointer, expression, '#')) return ParseNthWeekDay(field, ref pointer, expression, dayOfWeek, ref flags, out nthWeekDay);

            var bits = ParseRange(field, ref pointer, expression, dayOfWeek, ref flags);
            if (Accept(ref pointer, expression, ',')) bits |= ParseList(field, ref pointer, expression, ref flags);

            return bits;
        }

#if !NET40

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private static long ParseStar(CronField field, ref int pointer, string expression)
        {
            return Accept(ref pointer, expression, '/')
                ? ParseStep(field, ref pointer, expression, field.First, field.Last)
                : field.AllBits;
        }

#if !NET40
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private static long ParseList(CronField field, ref int pointer, string expression, ref CronExpressionFlag flags)
        {
            var num = ParseValue(field, ref pointer, expression);
            var bits = ParseRange(field, ref pointer, expression, num, ref flags);

            do
            {
                if (!Accept(ref pointer, expression, ',')) return bits;

                bits |= ParseList(field, ref pointer, expression, ref flags);
            } while (true);
        }

        private static long ParseRange(CronField field, ref int pointer, string expression, int low, ref CronExpressionFlag flags)
        {
            if (!Accept(ref pointer, expression, '-'))
            {
                if (!Accept(ref pointer, expression, '/')) return GetBit(low);

                if (field.CanDefineInterval) flags |= CronExpressionFlag.Interval;
                return ParseStep(field, ref pointer, expression, low, field.Last);
            }

            if (field.CanDefineInterval) flags |= CronExpressionFlag.Interval;

            var high = ParseValue(field, ref pointer, expression);
            if (Accept(ref pointer, expression, '/')) return ParseStep(field, ref pointer, expression, low, high);
            return GetBits(field, low, high, 1);
        }

        private static long ParseStep(CronField field, ref int pointer, string expression, int low, int high)
        {
            // Get the step size -- note: we don't pass the
            // names here, because the number is not an
            // element id, it's a step size.  'low' is
            // sent as a 0 since there is no offset either.
            var step = ParseNumber(field, ref pointer, expression, 1, field.Last);
            return GetBits(field, low, high, step);
        }

#if !NET40
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private static long ParseLastDayOfMonth(CronField field, ref int pointer, string expression, ref CronExpressionFlag flags, ref byte lastMonthOffset)
        {
            flags |= CronExpressionFlag.DayOfMonthLast;

            if (Accept(ref pointer, expression, '-')) lastMonthOffset = (byte)ParseNumber(field, ref pointer, expression, 0, field.Last - 1);
            if (AcceptCharacter(ref pointer, expression, 'W')) flags |= CronExpressionFlag.NearestWeekday;
            return field.AllBits;
        }

#if !NET40
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private static long ParseNthWeekDay(CronField field, ref int pointer, string expression, int dayOfWeek, ref CronExpressionFlag flags, out byte nthDayOfWeek)
        {
            nthDayOfWeek = (byte)ParseNumber(field, ref pointer, expression, MinNthDayOfWeek, MaxNthDayOfWeek);
            flags |= CronExpressionFlag.NthDayOfWeek;
            return GetBit(dayOfWeek);
        }


#if !NET40
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private static long ParseLastWeekDay(int dayOfWeek, ref CronExpressionFlag flags)
        {
            flags |= CronExpressionFlag.DayOfWeekLast;
            return GetBit(dayOfWeek);
        }

#if !NET40
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private static bool Accept(ref int pointer, string expression, char character)
        {
            if (GetChar(pointer, expression) == character)
            {
                pointer++;
                return true;
            }

            return false;
        }

#if !NET40
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private static bool AcceptCharacter(ref int pointer, string expression, char character)
        {
            if (ToUpper(GetChar(pointer, expression)) == character)
            {
                pointer++;
                return true;
            }

            return false;
        }

        private static int ParseNumber(CronField field, ref int pointer, string expression, int low, int high)
        {
            var num = GetNumber(ref pointer, expression, null);
            if (num == -1 || num < low || num > high)
            {
                ThrowFormatException(field, "Value must be a number between {0} and {1} (all inclusive).", low, high);
            }
            return num;
        }

#if !NET40
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private static int ParseValue(CronField field, ref int pointer, string expression)
        {
            var num = GetNumber(ref pointer, expression, field.Names);
            if (num == -1 || num < field.First || num > field.Last)
            {
                ThrowFormatException(field, "Value must be a number between {0} and {1} (all inclusive).", field.First, field.Last);
            }
            return num;
        }

#if !NET40
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private static StringBuilder AppendFieldValue(StringBuilder expressionBuilder, CronField field, long fieldValue)
        {
            if (field.AllBits == fieldValue) return expressionBuilder.Append('*');

            // Unset 7 bit for Day of week field because both 0 and 7 stand for Sunday.
            if (field == CronField.DaysOfWeek) fieldValue &= ~(1 << field.Last);

            for (var i = GetFirstSet(fieldValue); ; i = GetFirstSet(fieldValue >> i << i))
            {
                expressionBuilder.Append(i);
                if (fieldValue >> ++i == 0) break;
                expressionBuilder.Append(',');
            }

            return expressionBuilder;
        }

#if !NET40
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private StringBuilder AppendDayOfMonth(StringBuilder expressionBuilder, int domValue)
        {
            if (HasFlag(CronExpressionFlag.DayOfMonthLast))
            {
                expressionBuilder.Append('L');
                if (_lastMonthOffset != 0) expressionBuilder.Append($"-{_lastMonthOffset}");
            }
            else
            {
                AppendFieldValue(expressionBuilder, CronField.DaysOfMonth, (uint)domValue);
            }

            if (HasFlag(CronExpressionFlag.NearestWeekday)) expressionBuilder.Append('W');

            return expressionBuilder;
        }

#if !NET40
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private void AppendDayOfWeek(StringBuilder expressionBuilder, int dowValue)
        {
            AppendFieldValue(expressionBuilder, CronField.DaysOfWeek, dowValue);

            if (HasFlag(CronExpressionFlag.DayOfWeekLast)) expressionBuilder.Append('L');
            else if (HasFlag(CronExpressionFlag.NthDayOfWeek)) expressionBuilder.Append($"#{_nthDayOfWeek}");
        }

#if !NET40
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private static long GetBits(CronField field, int num1, int num2, int step)
        {
            if (num2 < num1) return GetReversedRangeBits(field, num1, num2, step);
            if (step == 1) return (1L << (num2 + 1)) - (1L << num1);

            return GetRangeBits(num1, num2, step);
        }

#if !NET40
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private static long GetRangeBits(int low, int high, int step)
        {
            var bits = 0L;
            for (var i = low; i <= high; i += step)
            {
                SetBit(ref bits, i);
            }
            return bits;
        }

#if !NET40
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private static long GetReversedRangeBits(CronField field, int num1, int num2, int step)
        {
            var high = field.Last;
            // Skip one of sundays.
            if (field == CronField.DaysOfWeek) high--;

            var bits = GetRangeBits(num1, high, step);

            num1 = field.First + step - (high - num1) % step - 1;
            return bits | GetRangeBits(num1, num2, step);
        }

#if !NET40
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private static long GetBit(int num1)
        {
            return 1L << num1;
        }

        private static int GetNumber(ref int pointer, string expression, int[] names)
        {
            if (IsDigit(pointer, expression))
            {
                var num = GetNumeric(GetChar(pointer++, expression));

                if (!IsDigit(pointer, expression)) return num;

                num = num * 10 + GetNumeric(GetChar(pointer++, expression));

                if (!IsDigit(pointer, expression)) return num;
                return -1;
            }

            if (names == null) return -1;

            if (!IsLetter(pointer, expression)) return -1;
            var buffer = ToUpper(GetChar(pointer++, expression));

            if (!IsLetter(pointer, expression)) return -1;
            buffer |= ToUpper(GetChar(pointer++, expression)) << 8;

            if (!IsLetter(pointer, expression)) return -1;
            buffer |= ToUpper(GetChar(pointer++, expression)) << 16;

            var length = names.Length;

            for (var i = 0; i < length; i++)
            {
                if (buffer == names[i])
                {
                    return i;
                }
            }

            return -1;
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowFormatException(CronField field, string format, params object[] args)
        {
            throw new CronFormatException(field, String.Format(format, args));
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowFormatException(string format, params object[] args)
        {
            throw new CronFormatException(String.Format(format, args));
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowFromShouldBeLessThanToException(string fromName, string toName)
        {
            throw new ArgumentException($"The value of the {fromName} argument should be less than the value of the {toName} argument.", fromName);
        }

        [MethodImpl(MethodImplOptions.NoInlining)]
        private static void ThrowWrongDateTimeKindException(string paramName)
        {
            throw new ArgumentException("The supplied DateTime must have the Kind property set to Utc", paramName);
        }

#if !NET40
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private static bool GetBit(long value, int index)
        {
            return (value & (1L << index)) != 0;
        }

#if !NET40
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private static void SetBit(ref long value, int index)
        {
            value |= 1L << index;
        }

#if !NET40
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private static bool IsEndOfString(int pointer, string expression)
        {
            var code = GetChar(pointer, expression);
            return code == '\0';
        }

#if !NET40
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private static bool IsWhiteSpace(int pointer, string expression)
        {
            var code = GetChar(pointer, expression);
            return code == '\t' || code == ' ';
        }

#if !NET40
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private static bool IsDigit(int pointer, string expression)
        {
            var code = GetChar(pointer, expression);
            return code >= 48 && code <= 57;
        }

#if !NET40
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private static bool IsLetter(int pointer, string expression)
        {
            var code = GetChar(pointer, expression);
            return (code >= 65 && code <= 90) || (code >= 97 && code <= 122);
        }

#if !NET40
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private static int GetNumeric(int code)
        {
            return code - 48;
        }

#if !NET40
        [MethodImpl(MethodImplOptions.AggressiveInlining)]
#endif
        private static int ToUpper(int code)
        {
            if (code >= 97 && code <= 122)
            {
                return code - 32;
            }

            return code;
        }
    }
}