/**
 * @author Stefano Ceccotti
*/

package gossiping.utils;

import java.io.InputStream;
import java.net.URL;

/**
 * A resource location that searches the classpath.
*/
public class ClasspathLocation implements ResourceLocation
{
	/**
	 * @see org.newdawn.slick.util.ResourceLocation#getResource(java.lang.String)
	*/
	public URL getResource( final String ref )
	{
		String cpRef = ref.replace( '\\', '/' );
		return ResourceLoader.class.getClassLoader().getResource( cpRef );
	}

	/**
	 * @see org.newdawn.slick.util.ResourceLocation#getResourceAsStream(java.lang.String)
	*/
	public InputStream getResourceAsStream( final String ref )
	{
		String cpRef = ref.replace( '\\', '/' );
		return ResourceLoader.class.getClassLoader().getResourceAsStream( cpRef );	
	}
}