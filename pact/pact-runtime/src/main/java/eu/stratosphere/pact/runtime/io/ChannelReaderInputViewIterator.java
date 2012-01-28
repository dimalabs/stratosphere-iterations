package eu.stratosphere.pact.runtime.io;

import java.io.EOFException;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;

import eu.stratosphere.nephele.services.iomanager.BlockChannelReader;
import eu.stratosphere.nephele.services.iomanager.Channel;
import eu.stratosphere.nephele.services.iomanager.IOManager;
import eu.stratosphere.nephele.services.memorymanager.MemorySegment;
import eu.stratosphere.pact.common.util.MutableObjectIterator;
import eu.stratosphere.pact.runtime.plugable.TypeAccessorsV2;


/**
 *
 *
 * @author Stephan Ewen (stephan.ewen@tu-berlin.de)
 */
public class ChannelReaderInputViewIterator<E> implements MutableObjectIterator<E>
{
	private final ChannelReaderInputViewV2 inView;
	
	private final TypeAccessorsV2<E> accessors;
	
	private final List<MemorySegment> freeMemTarget;
	
	
	public ChannelReaderInputViewIterator(IOManager ioAccess, Channel.ID channel, List<MemorySegment> segments,
			List<MemorySegment> freeMemTarget, TypeAccessorsV2<E> accessors, int numBlocks)
	throws IOException
	{
		this(ioAccess, channel, new LinkedBlockingQueue<MemorySegment>(), segments, freeMemTarget, accessors, numBlocks);
	}
		
	public ChannelReaderInputViewIterator(IOManager ioAccess, Channel.ID channel,  LinkedBlockingQueue<MemorySegment> returnQueue,
			List<MemorySegment> segments, List<MemorySegment> freeMemTarget, TypeAccessorsV2<E> accessors, int numBlocks)
	throws IOException
	{
		this(ioAccess.createBlockChannelReader(channel, returnQueue), returnQueue,
			segments, freeMemTarget, accessors, numBlocks);
	}
		
	public ChannelReaderInputViewIterator(BlockChannelReader reader, LinkedBlockingQueue<MemorySegment> returnQueue,
			List<MemorySegment> segments, List<MemorySegment> freeMemTarget, TypeAccessorsV2<E> accessors, int numBlocks)
	throws IOException
	{
		this.accessors = accessors;
		this.freeMemTarget = freeMemTarget;
		this.inView = new ChannelReaderInputViewV2(reader, segments, numBlocks, false);
	}

	/* (non-Javadoc)
	 * @see java.util.Iterator#next()
	 */
	@Override
	public boolean next(E target) throws IOException
	{
		try {
			this.accessors.deserialize(target, this.inView);
			return true;
		} catch (EOFException eofex) {
			final List<MemorySegment> freeMem = this.inView.close();
			this.freeMemTarget.addAll(freeMem);
			return false;
		}
	}
}
