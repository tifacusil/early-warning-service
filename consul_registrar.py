import logging
import socket

import consul

import config


logger = logging.getLogger("early_warning.consul")


def _default_address() -> str:
    """Direccion IP local alcanzable, usada cuando no se configura CONSUL_SERVICE_ADDRESS."""
    sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
    try:
        # No envia trafico real; solo fuerza al SO a elegir la interfaz de salida.
        sock.connect((config.CONSUL_HOST, config.CONSUL_PORT))
        return sock.getsockname()[0]
    except OSError:
        return socket.gethostbyname(socket.gethostname())
    finally:
        sock.close()


class ConsulRegistrar:
    def __init__(self) -> None:
        self._client = consul.Consul(
            host=config.CONSUL_HOST,
            port=config.CONSUL_PORT,
            scheme=config.CONSUL_SCHEME,
            token=config.CONSUL_TOKEN or None,
        )
        self._address = config.CONSUL_SERVICE_ADDRESS or _default_address()
        self._port = config.CONSUL_SERVICE_PORT
        self._service_id = config.CONSUL_SERVICE_ID or f"{config.CONSUL_SERVICE_NAME}-{self._address}-{self._port}"

    def register(self) -> None:
        check = consul.Check.http(
            url=f"{config.CONSUL_SCHEME}://{self._address}:{self._port}/health",
            interval=config.CONSUL_HEALTH_INTERVAL,
            timeout=config.CONSUL_HEALTH_TIMEOUT,
            deregister=config.CONSUL_DEREGISTER_AFTER,
        )
        self._client.agent.service.register(
            name=config.CONSUL_SERVICE_NAME,
            service_id=self._service_id,
            address=self._address,
            port=self._port,
            tags=config.CONSUL_TAGS or None,
            check=check,
        )
        logger.info(
            "consul_registered id=%s addr=%s:%s agent=%s:%s",
            self._service_id,
            self._address,
            self._port,
            config.CONSUL_HOST,
            config.CONSUL_PORT,
        )

    def deregister(self) -> None:
        try:
            self._client.agent.service.deregister(self._service_id)
            logger.info("consul_deregistered id=%s", self._service_id)
        except Exception as exc:
            logger.warning("consul_deregister_failed id=%s err=%s", self._service_id, exc)
