# syntax=docker/dockerfile:1

# =============================================================================================
# IronFlow dashboard image. Build the Vite app to static assets, then serve them from Nginx.
# Nginx also reverse-proxies /api and /actuator to the engine, so the browser talks to a single
# origin and there is no CORS to configure - the dev-time Vite proxy's production equivalent.
# =============================================================================================

# ---- Build stage ----------------------------------------------------------------------------
FROM node:22-alpine AS build
WORKDIR /app

# Install deps against the lockfile first, cached separately from source.
COPY package.json package-lock.json ./
RUN npm ci

COPY . .
# tsc -b && vite build. A type error fails the image build, which is the correct gate: a
# dashboard that does not type-check should never ship.
RUN npm run build

# ---- Runtime stage --------------------------------------------------------------------------
FROM nginx:1.27-alpine AS runtime

# Static assets.
COPY --from=build /app/dist /usr/share/nginx/html
# SPA + reverse-proxy config.
COPY nginx.conf /etc/nginx/conf.d/default.conf

EXPOSE 80

HEALTHCHECK --interval=10s --timeout=3s --start-period=5s --retries=3 \
    CMD ["sh", "-c", "wget -qO- http://localhost/ >/dev/null 2>&1 || exit 1"]
