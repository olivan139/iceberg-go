#pragma once

#include <stdint.h>

#ifdef __cplusplus
extern "C" {
#endif

/*
 * Opaque identifier that refers to a Go map[string]string created inside the
 * runtime. Use the helper functions below to populate the map before installing
 * a Prometheus metrics provider.
 */
typedef uintptr_t iceberg_metrics_property_map;

/* Go-exported entry points. */
extern uintptr_t new_property_map(void);
extern void delete_map(uintptr_t handle);
extern void add_map_entry(uintptr_t handle, const char *key, const char *value);
extern char *install_prometheus_metrics_provider(uintptr_t handle);
extern char *shutdown_metrics_provider(void);
extern void disable_metrics(void);
extern void free_c_string(char *str);

/*
 * Convenience wrappers that provide a stable API for C/C++ callers.
 */
static inline iceberg_metrics_property_map iceberg_metrics_new_property_map(void) {
        return new_property_map();
}

static inline void iceberg_metrics_delete_property_map(iceberg_metrics_property_map map) {
        delete_map(map);
}

static inline void iceberg_metrics_set_property(iceberg_metrics_property_map map,
                                                const char *key,
                                                const char *value) {
        add_map_entry(map, key, value);
}

/*
 * Installs the Prometheus provider using the supplied properties. The caller is
 * responsible for freeing the returned error string (if any) with
 * iceberg_metrics_free_error(). Returns NULL on success.
 */
static inline char *iceberg_metrics_install_prometheus_provider(iceberg_metrics_property_map map) {
        return install_prometheus_metrics_provider(map);
}

/*
 * Shuts down the active provider. Returns NULL on success. Free errors with
 * iceberg_metrics_free_error().
 */
static inline char *iceberg_metrics_shutdown_provider(void) {
        return shutdown_metrics_provider();
}

static inline void iceberg_metrics_disable(void) {
        disable_metrics();
}

static inline void iceberg_metrics_free_error(char *err) {
        free_c_string(err);
}

/*
 * Example usage:
 *
 *      iceberg_metrics_property_map props = iceberg_metrics_new_property_map();
 *      iceberg_metrics_set_property(props, "service.name", "iceberg-worker");
 *      iceberg_metrics_set_property(props, "service.version", "1.2.3");
 *      iceberg_metrics_set_property(props, "prometheus.handler_path", "/metrics");
 *      char *err = iceberg_metrics_install_prometheus_provider(props);
 *      iceberg_metrics_delete_property_map(props);
 *      if (err != NULL) {
 *              fprintf(stderr, "failed to install metrics: %s\n", err);
 *              iceberg_metrics_free_error(err);
 *      }
 */

#ifdef __cplusplus
} /* extern "C" */
#endif

