FROM oven/bun:1.4.2-debian
LABEL org.wsbench.runtime="bun" org.wsbench.implementation="Bun 1.4.2 + Bun.serve"
WORKDIR /app
COPY package.json server.ts room.ts ./
ENV PORT=8080 CORES=1 NODE_ENV=production
EXPOSE 8080
ENTRYPOINT ["bun", "run", "server.ts"]
