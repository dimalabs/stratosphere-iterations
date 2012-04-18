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

package eu.stratosphere.nephele.visualization.swt;

import java.util.Iterator;

import org.eclipse.swt.graphics.Color;
import org.eclipse.swt.graphics.Device;
import org.eclipse.swt.graphics.GC;
import org.eclipse.swt.widgets.Shell;

import eu.stratosphere.nephele.execution.ExecutionState;
import eu.stratosphere.nephele.managementgraph.ManagementVertex;

public class SWTVertex extends AbstractSWTVertex {

	private final static int SPACEFORGATESINPERCENT = 20;

	private final static int MINIMUMSIZEOFREPLAYICON = 40;

	private int gateHeight = 0;

	private final ManagementVertex managementVertex;

	private boolean replayMode = false;

	public SWTVertex(AbstractSWTComponent parent, ManagementVertex managementVertex) {
		super(parent);

		this.managementVertex = managementVertex;
	}

	@Override
	public void layout() {

		int numberOfInputGates = 0;
		int numberOfOutputGates = 0;

		Iterator<AbstractSWTComponent> it = getChildren();
		while (it.hasNext()) {

			AbstractSWTComponent child = it.next();
			if (child instanceof SWTGate) {
				SWTGate gate = (SWTGate) child;
				if (gate.isInputGate()) {
					numberOfInputGates++;
				} else {
					numberOfOutputGates++;
				}
			}
		}

		int numberOfLayoutedInputGates = 0;
		int numberOfLayoutedOutputGates = 0;
		int inputGateWidth = 0;
		if (numberOfInputGates > 0) {
			inputGateWidth = getWidth() / numberOfInputGates;
		}
		int outputGateWidth = 0;
		if (numberOfOutputGates > 0) {
			outputGateWidth = getWidth() / numberOfOutputGates;
		}

		this.gateHeight = (int) ((double) getHeight() * (SPACEFORGATESINPERCENT / 100.0f));
		it = getChildren();
		while (it.hasNext()) {

			AbstractSWTComponent child = it.next();
			if (child instanceof SWTGate) {
				SWTGate gate = (SWTGate) child;
				gate.setHeight(this.gateHeight);
				if (gate.isInputGate()) {
					gate.setX(getX() + numberOfLayoutedInputGates * inputGateWidth);
					gate.setY(getY() + getHeight() - this.gateHeight);
					gate.setWidth(inputGateWidth);

					numberOfLayoutedInputGates++;
				} else {
					gate.setX(getX() + numberOfLayoutedOutputGates * outputGateWidth);
					gate.setY(getY());
					gate.setWidth(outputGateWidth);

					numberOfLayoutedOutputGates++;
				}
			}

		}
	}

	@Override
	public boolean isSelectable() {
		return true;
	}

	@Override
	protected void paintInternal(final GC gc, final Device device) {

		final GroupVertexVisualizationData groupVertexVisualizationData = (GroupVertexVisualizationData) this.managementVertex
			.getGroupVertex().getAttachment();

		if (groupVertexVisualizationData.isCPUBottleneck()) {

			gc.setBackground(getBackgroundColor(device));
		} else {
			gc.setBackground(getBackgroundColor(device));
		}

		gc.fillRectangle(this.rect.x, this.rect.y, this.rect.width, this.rect.height);

		// Check for replay mode update
		final ExecutionState executionState = this.managementVertex.getExecutionState();
		switch (executionState) {
		case REPLAYING:
			this.replayMode = true;
			break;
		case RUNNING:
		case FINISHED:
		case CANCELED:
		case FAILED:
			this.replayMode = false;
			break;
		}

		if (this.replayMode) {
			paintReplayIcon(gc, device);
		}
	}

	private void paintReplayIcon(final GC gc, final Device device) {

		// Determine dimension of replay icon
		int dimension = getHeight();
		if (this.managementVertex.getNumberOfInputGates() > 0) {
			dimension -= this.gateHeight;
		}
		if (this.managementVertex.getNumberOfOutputGates() > 0) {
			dimension -= this.gateHeight;
		}
		dimension = Math.round((float) Math.min(dimension, getWidth()) * 0.7f);

		// Icon would be too small, do not draw it
		if (dimension < MINIMUMSIZEOFREPLAYICON) {
			return;
		}

		// Determine coordinates of icon
		final int x = getX() + (getWidth() / 2 - dimension / 2);
		int height = getHeight();
		final int numberOfInputGates = this.managementVertex.getNumberOfInputGates();
		final int numberOfOutputGates = this.managementVertex.getNumberOfOutputGates();
		if ((numberOfInputGates > 0 && numberOfOutputGates == 0)
			|| (numberOfInputGates == 0 && numberOfOutputGates > 0)) {
			height -= this.gateHeight;
		}
		int y = getY() + (height / 2 - dimension / 2);
		if (this.managementVertex.getNumberOfOutputGates() > 0) {
			y += this.gateHeight;
		}

		gc.setBackground(getReplayIconBackgroundColor(device));
		gc.fillOval(x, y, dimension, dimension);

		final int triangleWidth = Math.round((float) (dimension / 2) * 0.6f);
		final int triangleHeight = Math.round(dimension * 0.6f);

		final int triangleY = y + (dimension / 2 - triangleHeight / 2);
		final int leftTriangleX = x + (dimension / 2 - triangleWidth);
		final int rightTriangleX = x + (dimension / 2);

		paintReplayTriangle(gc, device, leftTriangleX, triangleY, triangleWidth, triangleHeight);
		paintReplayTriangle(gc, device, rightTriangleX, triangleY, triangleWidth, triangleHeight);
	}

	private void paintReplayTriangle(final GC gc, final Device device, final int x, final int y, final int width,
			final int height) {

		gc.setBackground(getReplayIconForegroundColor(device));

		final int[] polygon = new int[6];

		polygon[0] = x;
		polygon[1] = y + (height / 2);
		polygon[2] = x + width;
		polygon[3] = y;
		polygon[4] = x + width;
		polygon[5] = y + height;

		gc.fillPolygon(polygon);
	}

	private Color getReplayIconForegroundColor(Device device) {

		Color returnColor = null;

		switch (this.managementVertex.getExecutionState()) {
		case RUNNING:
			returnColor = ColorScheme.getGateRunningBorderColor(device);
			break;
		case REPLAYING:
			returnColor = ColorScheme.getGateReplayingBorderColor(device);
			break;
		case FINISHING:
			returnColor = ColorScheme.getGateFinishingBorderColor(device);
			break;
		case FINISHED:
			returnColor = ColorScheme.getGateFinishedBorderColor(device);
			break;
		case CANCELING:
		case CANCELED:
			returnColor = ColorScheme.getGateCancelBorderColor(device);
			break;
		case FAILED:
			returnColor = ColorScheme.getGateFailedBorderColor(device);
			break;
		default:
			returnColor = ColorScheme.getGateDefaultBorderColor(device);
			break;
		}

		return returnColor;
	}

	private Color getReplayIconBackgroundColor(Device device) {

		Color returnColor = null;

		switch (this.managementVertex.getExecutionState()) {
		case RUNNING:
			returnColor = ColorScheme.getGateRunningBackgroundColor(device);
			break;
		case REPLAYING:
			returnColor = ColorScheme.getGateReplayingBackgroundColor(device);
			break;
		case FINISHING:
			returnColor = ColorScheme.getGateFinishingBackgroundColor(device);
			break;
		case FINISHED:
			returnColor = ColorScheme.getGateFinishedBackgroundColor(device);
			break;
		case CANCELING:
		case CANCELED:
			returnColor = ColorScheme.getGateCancelBackgroundColor(device);
			break;
		case FAILED:
			returnColor = ColorScheme.getGateFailedBackgroundColor(device);
			break;
		default:
			returnColor = ColorScheme.getGateDefaultBackgroundColor(device);
			break;
		}

		return returnColor;
	}

	private Color getBackgroundColor(Device device) {

		Color returnColor = null;

		switch (this.managementVertex.getExecutionState()) {
		case RUNNING:
			returnColor = ColorScheme.getVertexRunningBackgroundColor(device);
			break;
		case REPLAYING:
			returnColor = ColorScheme.getVertexReplayingBackgroundColor(device);
			break;
		case FINISHING:
			returnColor = ColorScheme.getVertexFinishingBackgroundColor(device);
			break;
		case FINISHED:
			returnColor = ColorScheme.getVertexFinishedBackgroundColor(device);
			break;
		case CANCELING:
		case CANCELED:
			returnColor = ColorScheme.getVertexCancelBackgroundColor(device);
			break;
		case FAILED:
			returnColor = ColorScheme.getVertexFailedBackgroundColor(device);
			break;
		default:
			returnColor = ColorScheme.getVertexDefaultBackgroundColor(device);
			break;
		}

		return returnColor;
	}

	@Override
	public SWTToolTip constructToolTip(Shell parentShell, SWTToolTipCommandReceiver commandReceiver, int x, int y) {

		return new SWTVertexToolTip(parentShell, commandReceiver, this.managementVertex, x, y);
	}
}
