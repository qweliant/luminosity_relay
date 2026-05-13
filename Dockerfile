FROM ghcr.io/gleam-lang/gleam:nightly-erlang

WORKDIR /app
COPY . .

# Compile dependencies and bundle the standalone production release
RUN gleam export erlang-shipment

# Expose the internal listening port defined in your main() function
ENV PORT=4444
EXPOSE 4444

# Point directly to the native generated entrypoint script
ENTRYPOINT ["/app/build/erlang-shipment/entrypoint.sh"]
CMD ["run"]