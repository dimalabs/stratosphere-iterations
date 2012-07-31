package eu.stratosphere.util.dag;

import java.util.EnumSet;

class ASCIIConnectorProvider implements ConnectorProvider {
	@Override
	public String getConnectorString(final Route... connectors) {
		final EnumSet<Route> connectorSet = EnumSet.of(connectors[0], connectors);

		if (connectorSet.contains(Route.TOP_LEFT))
			return "/";
		if (connectorSet.contains(Route.TOP_RIGHT))
			return "\\";
		if (connectorSet.contains(Route.RIGHT_DOWN))
			return "/";
		if (connectorSet.contains(Route.LEFT_DOWN))
			return "\\";
		if (connectorSet.contains(Route.TOP_DOWN))
			return "|";
		return "-";
	};
}