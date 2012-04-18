/***********************************************************************************************************************
 *
 * Copyright (C) 2010 by the Stratosphere project (http://stratosphere.eu)
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License is distributed on
 * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 **********************************************************************************************************************/

package eu.stratosphere.nephele.plugins;

import eu.stratosphere.nephele.configuration.Configuration;

/**
 * This abstract class must be inherited by each plugin for Nephele. It specifies how to instantiate the individual
 * plugin components and provides access to the plugin environment, for example the plugin configuration.
 * <p>
 * This class is thread-safe.
 * 
 * @author warneke
 */
public abstract class AbstractPluginLoader {

	/**
	 * The name of the configuration as specified in the plugin configuration file
	 */
	private final String pluginName;

	/**
	 * The configuration for this plugin.
	 */
	private final Configuration pluginConfiguration;

	/**
	 * A service to lookup the location of the plugin's remote components.
	 */
	private final PluginLookupService pluginLookupService;

	/**
	 * Constructs the plugin loader.
	 * 
	 * @param pluginName
	 *        the name of the plugin as specified in the plugin configuration file
	 * @param pluginConfiguration
	 *        the plugin configuration
	 * @param pluginLookupService
	 *        a service to lookup the location of the plugin's remote components
	 */
	public AbstractPluginLoader(final String pluginName, final Configuration pluginConfiguration,
			final PluginLookupService pluginLookupService) {

		this.pluginName = pluginName;
		this.pluginConfiguration = pluginConfiguration;
		this.pluginLookupService = pluginLookupService;
	}

	/**
	 * Returns the {@link Configuration} for this plugin.
	 * 
	 * @return the {@link Configuration} for this plugin
	 */
	protected final Configuration getPluginConfiguration() {

		return this.pluginConfiguration;
	}

	/**
	 * Returns the name of the plugin as specified in the plugin configuration file.
	 * 
	 * @return the anem of the plugin as specified in the plugin configuration file
	 */
	final String getPluginName() {

		return this.pluginName;
	}

	/**
	 * Returns a service through which a plugin can determine the location of its remote components.
	 * 
	 * @return a service through which a plugin can determine the location of its remote components
	 */
	protected final PluginLookupService getPluginLookupService() {

		return this.pluginLookupService;
	}

	/**
	 * Returns an ID which uniquely specifies the plugin.
	 * 
	 * @return an ID which uniquely specified the plugin
	 */
	public abstract PluginID getPluginID();

	/**
	 * Loads and returns the plugin component which is supposed to run inside Nephele's {@link JobManager}.
	 * 
	 * @return the {@link JobManager} plugin component or <code>null</code> if this plugin does not provide such a
	 *         component.
	 */
	public abstract JobManagerPlugin getJobManagerPlugin();

	/**
	 * Loads and returns the plugin component which is supposed to run inside Nephele's {@link TaskManager}.
	 * 
	 * @return the {@link TaskManager} plugin component or <code>null</code> if this plugin does not provide such a
	 *         component.
	 */
	public abstract TaskManagerPlugin getTaskManagerPlugin();
}
