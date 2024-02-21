/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#pragma once

#include "error_codes.h"

#include <cstdint>
#include <exception>
#include <optional>
#include <string>

namespace ignite {

/**
 * Ignite Error.
 */
class ignite_error : public std::exception {
public:
    // Default
    ignite_error() = default;

    /**
     * Constructor.
     *
     * @param message Message.
     */
    explicit ignite_error(std::string message) noexcept
        : m_message(std::move(message)) {} // NOLINT(bugprone-throw-keyword-missing)

    /**
     * Constructor.
     *
     * @param message Message.
     */
    explicit ignite_error(std::string message, std::int32_t flags) noexcept
        : m_message(std::move(message))
        , m_flags(flags) {} // NOLINT(bugprone-throw-keyword-missing)

    /**
     * Constructor.
     *
     * @param statusCode Status code.
     * @param message Message.
     */
    explicit ignite_error(error::code code, std::string message) noexcept
        : m_status_code(code)
        , m_message(std::move(message)) {} // NOLINT(bugprone-throw-keyword-missing)

    /**
     * Constructor.
     *
     * @param statusCode Status code.
     * @param message Message.
     * @param ver Version.
     */
    explicit ignite_error(error::code code, std::string message, std::optional<std::int32_t> ver) noexcept
        : m_status_code(code)
        , m_message(std::move(message)) // NOLINT(bugprone-throw-keyword-missing)
        , m_version(ver) {}

    /**
     * Constructor.
     *
     * @param statusCode Status code.
     * @param message Message.
     * @param cause Error cause.
     */
    explicit ignite_error(error::code code, std::string message, std::exception_ptr cause) noexcept
        : m_status_code(code)
        , m_message(std::move(message))
        , m_cause(std::move(cause)) {} // NOLINT(bugprone-throw-keyword-missing)

    /**
     * Get error message.
     */
    [[nodiscard]] char const *what() const noexcept override { return m_message.c_str(); }

    /**
     * Get error message as std::string.
     */
    [[nodiscard]] const std::string &what_str() const noexcept { return m_message; }

    /**
     * Get status code.
     *
     * @return Status code.
     */
    [[nodiscard]] error::code get_status_code() const noexcept { return m_status_code; }

    /**
     * Get error cause.
     *
     * @return Error cause. Can be empty.
     */
    [[nodiscard]] std::exception_ptr get_cause() const noexcept { return m_cause; }

    /**
     * Get flags.
     * Internal method.
     *
     * @return Flags.
     */
    [[nodiscard]] std::int32_t get_flags() const noexcept { return m_flags; }

    /**
     * Get expected schema version.
     * Internal method.
     *
     * @return Expected schema version.
     */
    [[nodiscard]] std::optional<std::int32_t> get_schema_version() const noexcept { return m_version; }

private:
    /** Status code. */
    error::code m_status_code{error::code::GENERIC};

    /** Message. */
    std::string m_message;

    /** Cause. */
    std::exception_ptr m_cause;

    /** Flags. */
    std::int32_t m_flags{0};

    /** Schema version. */
    std::optional<std::int32_t> m_version{};
};

} // namespace ignite
