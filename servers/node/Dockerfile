FROM node:24-bookworm-slim AS build
WORKDIR /app
COPY package.json package-lock.json ./
RUN npm ci --omit=dev --no-fund --no-audit

FROM node:24-bookworm-slim
LABEL org.wsbench.runtime="node" org.wsbench.implementation="Node.js 24 + ws 8.18"
WORKDIR /app
COPY --from=build /app/node_modules ./node_modules
COPY package.json server.js room.js ./
ENV PORT=8080 CORES=1 NODE_ENV=production
EXPOSE 8080
ENTRYPOINT ["node", "server.js"]
